<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\DataQueries\SqlDataQuery;
use axenox\ETL\Common\Traits\IncrementalSqlWhereTrait;

class SQLSelectValidator extends SQLRunner
{
    use IncrementalSqlWhereTrait;
    
    private $maxRows = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getSql()
     */
    protected function getSql() : string
    {
        if ($incr = $this->getIncrementalWhere()) {
            $where = 'WHERE ' . $incr;
        }
        
        return <<<SQL

SELECT * FROM [#to_object_address#] {$where};

SQL;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::countAffectedRows()
     */
    protected function countAffectedRows(SqlDataQuery $query) : ?int
    {
        return count($query->getResultArray());
    }
    
    public function setMaxRows(int $value) : SQLSelectValidator
    {
        $this->maxRows = $value;
        return $this;
    }
    
    protected function getMaxRows() : ?int
    {
        return $this->maxRows;
    }        
}