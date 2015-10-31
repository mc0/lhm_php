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
    protected $nextToInsert;

    /** @var integer */
    protected $limit;

    /** @var integer */
    protected $start;

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
                'strideMax' => 20000000,
                'targetQuerySeconds' => 0.75
            ];

        $this->primaryKey = $this->adapter->quoteColumnName($this->sqlHelper->extractPrimaryKey($this->origin));

        $this->nextToInsert = $this->start = $this->selectStart();
        $this->limit = $this->selectLimit();

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
        while ($this->nextToInsert < $this->limit || ($this->nextToInsert == 1 && $this->start == 1)) {
            $top = $this->top($stride);
            $bottom = $this->bottom();
            $queries = $this->copy($bottom, $top);

            $query = "$queries[0] $queries[1]";
            $this->getLogger()->debug($query);

            $startTime = microtime(true);
            $result = $this->adapter->query($query);
            $rowCount = $result->rowCount();
            $totalRows += $rowCount;
            $endTime = microtime(true);
            $timing = $endTime - $startTime;
            $this->nextToInsert = $top + 1;

            if ($endTime - $lastUpdate > 10) {
                $msg = "Copied $totalRows of an estimated $estRows rows.\n";
                $this->getLogger()->info($msg);
                $msg = "There is a stride of $stride and a goal of $targetQuerySeconds.";
                $this->getLogger()->info($msg);
                $lastUpdate = $endTime;
            }

            if ($rowCount === 0) {
                // find the next row
                $result = $this->adapter->query($queries[2]);
                $row = $result->fetch(PDO::FETCH_NUM);
                if (!empty($row) && !empty($row[0])) {
                    $this->nextToInsert = $row[0];
                }
            } else {
                $rowsPerSec = $rowCount / $timing;
                $trendWeight = (0.5 * ($rowsPerSec - $weightedAvgPace));
                $weightedAvgPace = $weightedAvgPace + $trendWeight;
                if (!empty($weightedAvgPace)) {
                    $targetStride = $targetQuerySeconds * $weightedAvgPace;
                    $ratio =  max(min($targetStride / $stride, 1.25), 0.5);
                    $stride = $ratio * $stride;
                    $stride = max(min($stride, $strideMax), $strideStart);
                    $stride = (int)floor($stride);
                }
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

    protected function top($stride)
    {
        return min(($this->nextToInsert + $stride - 1), $this->limit);
    }

    protected function bottom()
    {
        return $this->nextToInsert;
    }

    protected function selectStart()
    {
        $name = $this->adapter->quoteTableName($this->origin->getName());
        $start = $this->adapter->fetchRow("SELECT MIN(id) FROM {$name}")[0];

        return (int)$start;
    }

    protected function selectLimit()
    {
        $name = $this->adapter->quoteTableName($this->origin->getName());
        $limit = $this->adapter->fetchRow("SELECT MAX(id) FROM {$name}")[0];

        return (int)$limit;
    }

    /**
     * @param integer $lowest
     * @param integer $highest
     * @return string
     */
    protected function copy($lowest, $highest)
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

        $queries = array(
            "INSERT IGNORE INTO {$destinationName} ({$destinationColumns})",
            "SELECT {$originColumns} FROM {$originName}
             WHERE {$originName}.{$this->primaryKey} BETWEEN {$lowest} AND {$highest}",
            "SELECT {$originName}.{$this->primaryKey} FROM {$originName}
             WHERE {$originName}.{$this->primaryKey} >= {$lowest}
             LIMIT 1",
        );

        return $queries;
    }
}
