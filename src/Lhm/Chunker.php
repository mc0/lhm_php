<?php

namespace Lhm;

use Phinx\Db\Adapter\AdapterFactory;
use Phinx\Db\Adapter\AdapterInterface;
use InvalidArgumentException;
use PDO;
use PDOException;


class Chunker extends Command
{
    private const SLAVE_LAG_CHECK_FREQUENCY = 30; // seconds

    private const MAX_ALLOWED_SLAVE_LAG = 10; // seconds
    private const MIN_ALLOWED_SLAVE_LAG = 5; // seconds

    private const MIN_DELAY_MICRO_S = 1000;
    private const MAX_DELAY_MICRO_S = 10 * 1000 * 1000; // 10s

    /**
     * @var AdapterInterface
     */
    protected $adapter;

    /**
     * @var AdapterInterface[]
     */
    protected $slaveAdapters;

    /**
     * @var Table
     */
    protected $origin;

    /**
     * @var Table
     */
    protected $destination;

    /**
     * @var SqlHelper
     */
    protected $sqlHelper;

    /** @var integer */
    protected $currentOffset;

    /** @var integer */
    protected $limit;

    /** @var string */
    protected $primaryKey;

    /** @var array */
    protected $options;

    /**
     * @var Intersection
     */
    protected $intersection;

    /**
     * @var array
     */
    protected $ignoreWarningCodes = [
        # Error: 1062 (ER_DUP_ENTRY)
        # Message: Duplicate entry '%ld' for key '%s'
        1062 => 1,
        # Error: 1265 (WARN_DATA_TRUNCATED)
        # Message: Data truncated for column '%s' at row %ld
        1265 => 1,
        # Error: 1592 (ER_BINLOG_UNSAFE_STATEMENT)
        # Message: Statement may not be safe to log in statement format.
        1592 => 1,
    ];

    /**
     * @var array
     */
    protected $retryErrors = [
        'Deadlock found',
        'Lock wait timeout exceeded',
        'Query execution was interrupted',
    ];

    /**
     * @param AdapterInterface $adapter
     * @param \Phinx\Db\Table $origin
     * @param \Lhm\Table $destination
     * @param SqlHelper $sqlHelper
     * @param array $options
     *                      - `stride`
     *                          Size of chunk ( defaults to 2000 )
     */
    public function __construct(AdapterInterface $adapter, \Phinx\Db\Table $origin, \Lhm\Table $destination, SqlHelper $sqlHelper = null, array $options = [])
    {
        $this->adapter = $adapter;
        $this->origin = $origin;
        $this->destination = $destination;
        $this->sqlHelper = $sqlHelper ?: new SqlHelper($this->adapter);

        $this->options = $options + [
            'stride' => 2000,
            'strideMax' => 20000,
            'targetQuerySeconds' => 0.75,
        ];

        $this->primaryKey = $this->adapter->quoteColumnName($this->sqlHelper->extractPrimaryKey($this->origin));

        $adapterFactory = AdapterFactory::instance();

        $adapterOptions = $this->adapter->getOptions();

        if (!empty($adapterOptions['auto_detect_slaves'])) {
            $this->getLogger()->info("Attempting to auto-detect slave database connections");
            $res = $this->adapter->query('SELECT HOST FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND = \'Binlog Dump\'')->fetchAll();
            foreach ($res as $row) {
                list($hostname, $port) = explode(':', $row[0], 2);
                $slaveOptions = $adapterOptions;
                $slaveOptions['host'] = $hostname;
                $slaveOptions['port'] = $port;

                $this->slaveAdapters []= $adapterFactory->getAdapter('mysql', $slaveOptions);
                $this->getLogger()->info("Registered slave adapter for {$slaveOptions['host']}:{$slaveOptions['port']}");
            }
        }
        if (!empty($adapterOptions['slave_dbs'])) {
            foreach ($adapterOptions['slave_dbs'] as $slaveOptions) {
                // Override any missing values with ones from the base adapter
                $slaveOptions = array_merge($adapterOptions, $slaveOptions);

                $this->slaveAdapters []= $adapterFactory->getAdapter('mysql', $slaveOptions);
                $this->getLogger()->info("Registered slave adapter for {$slaveOptions['host']}:{$slaveOptions['port']}");
            }
        }

        $this->currentOffset = 0;
        $originName = $this->origin->getName();
        $this->limit = $this->getMaxPrimary($originName);

        $this->intersection = new Intersection($this->origin, $this->destination);
    }

    private function getMaxSlaveLag()
    {
        $maxLag = 0;
        foreach ($this->slaveAdapters as $slaveAdapter) {
            try {
                $result = $slaveAdapter->query('SHOW SLAVE STATUS')->fetchAll();
                foreach ($result as $row) {
                    $lag = $row['Seconds_Behind_Master'];
                    $maxLag = max($maxLag, $lag);
                }
            } catch (PDOException $e) {
                // expected when a backup slave is down for a backup
                // disconnect, we will reconnect next time we query
                $slaveAdapter->disconnect();
                $maxLag = self::MAX_ALLOWED_SLAVE_LAG + 1;
            } catch (InvalidArgumentException $e) {
                // expected when a backup slave is down for a backup
                $maxLag = self::MAX_ALLOWED_SLAVE_LAG + 1;
            }
        }
        $this->logger->info("Max slave lag at $maxLag");
        return $maxLag;
    }

    protected function execute()
    {
        $this->getLogger()->info("Copying data from `{$this->origin->getName()}` into `{$this->destination->getName()}`");

        $stride = $this->options['stride'];
        $strideMax = $this->options['strideMax'];
        $targetQuerySeconds = $this->options['targetQuerySeconds'];
        if (empty($targetQuerySeconds)) {
            $targetQuerySeconds = 0.75;
        }
        $weightedAvgPace = 0;

        // if we have fewer rows than the stride, just copy it all in one pass
        $estRows = $this->getEstimatedRows();
        if ($estRows < $stride) {
            $stride = $this->limit;
        }

        $strideStart = $stride;
        $initialStart = microtime(true);
        $lastUpdate = 0;
        $totalRows = 0;
        $lastSlaveLagCheck = 0;
        $delay = self::MIN_DELAY_MICRO_S;

        while ($this->currentOffset <= $this->limit || $this->currentOffset === 0) {
            // When we have known slave databases, we need to be careful not to introduce too much slave lag
            if (!empty($this->slaveAdapters)) {
                // Periodically check the lag levels
                if ($lastSlaveLagCheck < time() - self::SLAVE_LAG_CHECK_FREQUENCY) {
                    $slaveLag = $this->getMaxSlaveLag();
                    $lastSlaveLagCheck = time();

                    if ($slaveLag > self::MAX_ALLOWED_SLAVE_LAG && $delay < self::MAX_DELAY_MICRO_S) {
                        // slave lag is too high -- slow down
                        $delay = min($delay * 2, self::MAX_DELAY_MICRO_S);
                        $lastSlaveLagCheck = 0; // Also reset this so that it will be re-checked the next iteration
                        $this->logger->warning("Slave lag over max allowed, increasing per-chunk delay to $delay microseconds");
                    }
                    if ($slaveLag < self::MIN_ALLOWED_SLAVE_LAG && $delay > self::MIN_DELAY_MICRO_S) {
                        // slave lag is too low -- speed up
                        $delay = max($delay / 2, self::MIN_DELAY_MICRO_S);
                        $this->logger->warning("Slave lag recovering, decreasing per-chunk delay to $delay microseconds");
                    }
                }

                usleep($delay);
            }

            $query = $this->copy($this->currentOffset, $stride);
            $this->getLogger()->debug($query);

            $startTime = microtime(true);
            $i = 0;
            $rowCount = 0;
            while (true) {
                try {
                    $result = $this->adapter->query($query);
                    $rowCount = $result->rowCount();
                    break;
                } catch (PDOException $e) {
                    if (!$this->shouldRetry($e->getMessage())) {
                        throw $e;
                    }
                    if ($i > 10) {
                        throw $e;
                    }
                    usleep(max(min($i * 500, 1000), 75));
                }
                $i++;
            }
            $warnings = $this->getWarnings();
            $totalRows += $rowCount;
            $endTime = microtime(true);
            $timing = $endTime - $startTime;

            if ($endTime - $lastUpdate > 10) {
                $msg = "Copied $totalRows of an estimated $estRows rows.\n";
                $this->getLogger()->info($msg);
                $msg = "There is a stride of $stride and a goal of {$targetQuerySeconds} sec/query.";
                $this->getLogger()->info($msg);
                $lastUpdate = $endTime;
            }

            foreach ($warnings as $warning) {
                $code = $warning['Code'];
                if (!isset($this->ignoreWarningCodes[$code])) {
                    throw new \Exception("Database error returned: {$warning['Message']} $code");
                }
            }

            if (empty($warnings) && $rowCount === 0) {
                break;
            }
            $newOffset = $this->getNextChunkStart($this->currentOffset, $stride);
            if (is_null($newOffset)) {
                break;
            }
            if ($newOffset == $this->currentOffset) {
                $msg = "Offset $newOffset invalid: $this->currentOffset offset / $stride stride";
                $this->getLogger()->error($msg);
                throw new \Exception('infinite loop due to bad primary key?');
            }
            $this->currentOffset = $newOffset;

            $rowsPerSec = $rowCount / $timing;
            $trendWeight = (0.5 * ($rowsPerSec - $weightedAvgPace));
            $weightedAvgPace = $weightedAvgPace + $trendWeight;
            if (!empty($weightedAvgPace)) {
                $targetStride = $targetQuerySeconds * $weightedAvgPace;
                $ratio = max(min($targetStride / $stride, 3), 0.5);
                $stride = $ratio * $stride;
                $stride = max(min($stride, $strideMax), $strideStart);
                $stride = (int)ceil($stride);
            }
        }

        $totalTime = microtime(true) - $initialStart;
        $this->getLogger()->info("Copied $totalRows in $totalTime.\n");
    }

    /**
     * @return int
     */
    protected function getEstimatedRows()
    {
        $originName = $this->adapter->quoteTableName($this->origin->getName());

        $sql = "EXPLAIN SELECT * FROM $originName WHERE 1";
        $result = $this->adapter->query($sql);
        $row = $result->fetch(PDO::FETCH_ASSOC);

        return $row['rows'];
    }

    protected function getMaxPrimary($table)
    {
        $name = $this->adapter->quoteTableName($table);
        $sql = "SELECT MAX({$this->primaryKey})
                FROM {$name}";
        $max = $this->adapter->fetchRow($sql)[0];

        return (int)$max;
    }

    protected function getNextChunkStart($oldStart, $oldStride)
    {
        $originName = $this->adapter->quoteTableName($this->origin->getName());
        $sql = "SELECT {$this->primaryKey}
                FROM {$originName}
                WHERE {$this->primaryKey} >= $oldStart
                ORDER BY {$this->primaryKey} ASC
                LIMIT 1 OFFSET $oldStride";
        $row = $this->adapter->fetchRow($sql);
        if (empty($row)) {
            return null;
        }
        $next = $row[0];

        return (int)$next;
    }

    protected function getWarnings()
    {
        $warnings = $this->adapter->fetchAll('SHOW WARNINGS');
        return $warnings;
    }

    protected function shouldRetry($msg)
    {
        foreach ($this->retryErrors as $error) {
            if (strpos($msg, $error) !== false) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param integer $current
     * @param integer $stride
     * @return string
     */
    protected function copy($current, $stride)
    {
        $originName = $this->adapter->quoteTableName($this->origin->getName());
        $destinationName = $this->adapter->quoteTableName($this->destination->getName());

        $destinationColumns = implode(
            ',',
            $this->sqlHelper->quoteColumns($this->intersection->destination())
        );

        $originColumns = implode(
            ',',
            $this->sqlHelper->typedColumns(
                $originName,
                $this->sqlHelper->quoteColumns($this->intersection->origin())
            )
        );

        return implode(" ", [
            "INSERT LOW_PRIORITY IGNORE INTO {$destinationName} ({$destinationColumns})",
            "SELECT {$originColumns} FROM {$originName}
             WHERE {$originName}.{$this->primaryKey} >= {$current}
             ORDER BY {$originName}.{$this->primaryKey} ASC
             LIMIT $stride"
        ]);
    }
}
