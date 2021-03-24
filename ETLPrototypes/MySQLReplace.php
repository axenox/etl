<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Common\Traits\IncrementalSqlWhereTrait;
use axenox\ETL\Common\Traits\SqlColumnMappingsTrait;

class MySQLReplace extends SQLRunner
{
    use IncrementalSqlWhereTrait; 
    use SqlColumnMappingsTrait;
    
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
        $targetCols = '';
        $sourceCols = '';
        $where = '';
        
        foreach ($this->getColumnMappings() as $map) {
            $targetCols .= ($targetCols ? ', ' : '') . $map->getToSql();
            $sourceCols .= ($sourceCols ? ', ' : '') . $map->getFromSql();
        }
        
        if ($targetCols === '' || $sourceCols === '') {
            throw new RuntimeException('Cannot run ETL step "' . $this->getName() . '": no `column_mappings` defined!');
        }
        
        if ($runUidAlias = $this->getStepRunUidAttributeAlias()) {
            $targetCols .= ', ' . $this->getToObject()->getAttribute($runUidAlias)->getDataAddress();
            $sourceCols .= ', [#step_run_uid#]';
        }
        
        if ($incr = $this->getIncrementalWhere()) {
            $where = 'WHERE ' . $incr;
        }
        
        return <<<SQL

REPLACE INTO [#to_object_address#] 
    ({$targetCols}) 
    SELECT 
        {$sourceCols} 
    FROM [#from_object_address#]
    {$where};

SQL;
    }
}