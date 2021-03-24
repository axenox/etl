<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Common\Traits\SqlColumnMappingsTrait;
use axenox\ETL\Common\Traits\IncrementalSqlWhereTrait;

class MsSQLMerge extends SQLRunner
{    
   use SqlColumnMappingsTrait;
   use IncrementalSqlWhereTrait;
   
   private $mergeOnCondition = null;
   
   protected function getSql() : string
    {
        if ($customSql = parent::getSql()) {
            return $customSql;
        }
        
        $insertValues = '';
        $insertCols = '';
        $updates = '';
        
        foreach ($this->getColumnMappings() as $map) {
            $insertCols .= ($insertCols ? ', ' : '') . $map->getToSql();
            $insertValues .= ($insertValues ? ', ' : '') . "[#source#].{$map->getFromSql()}";
            $updates .= ($updates ? ', ' : '') . "[#target#].{$map->getToSql()} = [#source#].{$map->getFromSql()}";
        }
        
        if ($insertValues === '' || $insertCols === '') {
            throw new RuntimeException('Cannot run ETL step "' . $this->getName() . '": no `column_mappings` defined!');
        }
        
        if ($runUidAlias = $this->getStepRunUidAttributeAlias()) {
            $toSql = $this->getToObject()->getAttribute($runUidAlias)->getDataAddress();
            $insertCols .= ', ' . $toSql;
            $insertValues .= ', [#step_run_uid#]';
            $updates .= ($updates ? ', ' : '') . "[#target#].{$toSql} = [#step_run_uid#]";
        }
        
        $mergeCondition = $this->getSqlMergeOnCondition();
        
        if ($incr = $this->getIncrementalWhere()) {
            $mergeCondition .= ' AND ' . $incr;
        }
        
        return <<<SQL

MERGE [#to_object_address#] [#target#] USING [#from_object_address#] [#source#]
    ON ({$mergeCondition})
    WHEN MATCHED
        THEN UPDATE SET {$updates}
    WHEN NOT MATCHED
        THEN INSERT ($insertCols)
             VALUES ($insertValues);

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
            'source' => 'exfs',
            'target' => 'exft'
        ]);
    }
    
    /**
     * 
     * @return string
     */
    protected function getSqlMergeOnCondition() : string
    {
        return $this->mergeOnCondition;
    }
    
    /**
     * The SQL to use in the ON clause of the MERGE
     * 
     * @uxon-property sql_merge_on_condition
     * 
     * @param string $value
     * @return MsSQLMerge
     */
    protected function setSqlMergeOnCondition(string $value) : MsSQLMerge
    {
        $this->mergeOnCondition = $value;
        return $this;
    }
}