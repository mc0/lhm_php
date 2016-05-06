<?php

namespace Lhm;

use Phinx\Db\Adapter\AdapterInterface;
use PDO;


class Chunker extends Command
{
    /**
     * @var AdapterInterface
     */
    protected $adapter;

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
                'targetQuerySeconds' => 0.75
            ];

        $this->primaryKey = $this->adapter->quoteColumnName($this->sqlHelper->extractPrimaryKey($this->origin));

        $this->currentOffset = 0;
        $originName = $this->origin->getName();
        $this->limit = $this->getMaxPrimary($originName);

        $this->intersection = new Intersection($this->origin, $this->destination);
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
        while ($this->currentOffset < $this->limit || $this->currentOffset === 0) {
            $query = $this->copy($this->currentOffset, $stride);
            $this->getLogger()->debug($query);

            $startTime = microtime(true);
            $result = $this->adapter->query($query);
            $rowCount = $result->rowCount();
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

            if ($rowCount === 0) {
                break;
            }
            $newOffset = $this->getNextChunkStart($this->currentOffset, $stride);
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
        $next = $this->adapter->fetchRow($sql)[0];

        return (int)$next;
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
            "INSERT IGNORE INTO {$destinationName} ({$destinationColumns})",
            "SELECT {$originColumns} FROM {$originName}
             WHERE {$originName}.{$this->primaryKey} > {$current}
             ORDER BY {$originName}.{$this->primaryKey} ASC
             LIMIT $stride"
        ]);
    }
}
