<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Common\Traits\SqlColumnMappingsTrait;
use axenox\ETL\Common\Traits\SqlIncrementalWhereTrait;

class MsSQLMerge extends SQLRunner
{    
   use SqlColumnMappingsTrait;
   use SqlIncrementalWhereTrait;
   
   private $mergeOnCondition = null;
   
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

MERGE [#to_object_address#] [#target#] USING [#from_object_address#] [#source#]
    ON ([#merge_condition#] AND [#incremental_where#])
    WHEN MATCHED
        THEN UPDATE SET [#update_pairs#]
    WHEN NOT MATCHED
        THEN INSERT ([#insert_columns#])
             VALUES ([#insert_values#]);

SQL;
    }
    
    /**
     * Override the default MERGE statement to add a GROUP BY or other enhancements.
     * 
     * The default statement is
     * 
     * ```
     * MERGE [#to_object_address#] [#target#] USING [#from_object_address#] [#source#]
     *    ON ([#merge_condition#] AND [#incremental_where#])
     *    WHEN MATCHED
     *        THEN UPDATE SET [#update_pairs#]
     *    WHEN NOT MATCHED
     *        THEN INSERT ([#insert_columns#])
     *             VALUES ([#insert_values#]);
     *             
     * ```
     * 
     * @uxon-property sql
     * @uxon-type string
     * 
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::setSql()
     */
    protected function setSql(string $value) : SQLRunner
    {
        return parent::setSql($value);
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getPlaceholders()
     */
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
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
        
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult), [
            'source' => 'exfs',
            'target' => 'exft',
            'merge_condition' => $mergeCondition,
            'insert_values' => $insertValues,
            'insert_columns' => $insertCols,
            'update_pairs' => $updates,
            'incremental_where' => $this->getSqlIncrementalWhere() ?? '1'
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