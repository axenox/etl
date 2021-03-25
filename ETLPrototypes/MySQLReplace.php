<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Common\Traits\SqlIncrementalWhereTrait;
use axenox\ETL\Common\Traits\SqlColumnMappingsTrait;
use axenox\ETL\Interfaces\ETLStepResultInterface;

class MySQLReplace extends SQLRunner
{
    use SqlIncrementalWhereTrait; 
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
        
        return <<<SQL

REPLACE INTO [#to_object_address#] 
    ([#columns#]) 
    SELECT 
        [#selects#] 
    FROM [#from_object_address#]
    WHERE [#incremental_where#];

SQL;
    }
    
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        $targetCols = '';
        $sourceCols = '';
        
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
        
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult), [
            'columns' => $targetCols,
            'selects' => $sourceCols,
            'incremental_where' => $this->getSqlIncrementalWhere() ?? '1'
        ]);
    }
}