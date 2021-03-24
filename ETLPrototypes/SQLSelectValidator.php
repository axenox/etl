<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\DataQueries\SqlDataQuery;

class SQLSelectValidator extends SQLRunner
{
    private $maxRows = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getSql()
     */
    protected function getSql() : string
    {
        /*if ($runUidAlias = $this->getStepRunUidAttributeAlias()) {
            $runUidCol = $this->getToObject()->getAttribute($runUidAlias)->getDataAddress();
            $where = "WHERE $runUidCol = [#previuos_step_run_uid#]";
        }*/
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
    
    public function setMaxRows(int $value) : \SQLSelectValidator
    {
        $this->maxRows = $value;
        return $this;
    }
    
    protected function getMaxRows() : ?int
    {
        return $this->maxRows;
    }
}