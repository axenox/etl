<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\DataQueries\SqlDataQuery;

class SQLSelectValidator extends SQLRunner
{
    private $maxRows = null;
    
    private $incrementalWhere = null;
    
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
    
    protected function getIncrementalWhere() : ?string
    {
        return $this->incrementalWhere;
    }
    
    /**
     * SQL predicate for the WHERE statement that will take care of the `[#last_step_increment_value#]` placeholder.
     *
     * @uxon-property incremental_where
     * @uxon-type string
     *
     * @param string $value
     * @return MySQLReplace
     */
    protected function setIncrementalWhere(string $value) : SQLSelectValidator
    {
        $this->incrementalWhere = $value;
        return $this;
    }
        
}