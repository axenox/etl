<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\DataQueries\SqlDataQuery;
use axenox\ETL\Common\Traits\SqlIncrementalWhereTrait;
use axenox\ETL\Interfaces\ETLStepResultInterface;

class SQLSelectValidator extends SQLRunner
{
    use SqlIncrementalWhereTrait;
    
    private $maxRows = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getSql()
     */
    protected function getSql() : string
    {
        if ($customSql = parent::getSql()) {
            return $customSql;
        }
        
        return <<<SQL

SELECT * FROM [#to_object_address#] WHERE [#incremental_where#];

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
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getPlaceholders()
     */
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult), [
            'incremental_where' => $this->getSqlIncrementalWhere() ?? '(1=1)'
        ]);
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