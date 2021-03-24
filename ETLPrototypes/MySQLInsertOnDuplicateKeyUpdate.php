<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Common\Traits\SqlColumnMappingsTrait;
use axenox\ETL\Common\Traits\IncrementalSqlWhereTrait;

class MySQLInsertOnDuplicateKeyUpdate extends SQLRunner
{   
    use SqlColumnMappingsTrait;
    use IncrementalSqlWhereTrait;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\MySQLReplace::getSql()
     */
    protected function getSql() : string
    {
        if ($customSql = parent::getSql()) {
            return $customSql;
        }
        
        $insertSelects = '';
        $insertCols = '';
        $updates = '';
        
        foreach ($this->getColumnMappings() as $map) {
            $insertCols .= ($insertCols ? ', ' : '') . $map->getToSql();
            $insertSelects .= ($insertSelects ? ', ' : '') . "[#source#].{$map->getFromSql()}";
            $updates .= ($updates ? ', ' : '') . "{$map->getToSql()} = [#source#].{$map->getFromSql()}";
        }
        
        if ($insertSelects === '' || $insertCols === '') {
            throw new RuntimeException('Cannot run ETL step "' . $this->getName() . '": no `column_mappings` defined!');
        }
        
        if ($runUidAlias = $this->getStepRunUidAttributeAlias()) {
            $toSql = $this->getToObject()->getAttribute($runUidAlias)->getDataAddress();
            $insertCols .= ', ' . $toSql;
            $insertSelects .= ', [#step_run_uid#]';
            $updates .= ($updates ? ', ' : '') . "{$toSql} = [#step_run_uid#]";
        }
        
        if ($incr = $this->getIncrementalWhere()) {
            $where .= 'WHERE ' . $incr;
        }
        
        return <<<SQL

INSERT INTO [#to_object_address#]
        ($insertCols) 
    (SELECT 
        $insertSelects
        FROM [#from_object_address#] [#source#]
        {$where}
    )
    ON DUPLICATE KEY UPDATE 
        {$updates};

SQL;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getPlaceholders()
     */
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult), [
            'source' => 'exfsrc'
        ]);
    }
}