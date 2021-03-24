<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\RuntimeException;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Interfaces\ETLStepResultInterface;

class MsSQLMerge extends MySQLReplace
{    
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
            $insertValues .= ($insertValues ? ', ' : '') . $map->getToSql();
            $insertCols .= ($insertCols ? ', ' : '') . $map->getFromSql();
            $updates .= ($updates ? ', ' : '') . "[#merge_target#].{$map->getToSql()} = [#merge_source#].{$map->getFromSql}";
        }
        
        if ($insertValues === '' || $insertCols === '') {
            throw new RuntimeException('Cannot run ETL step "' . $this->getName() . '": no `column_mappings` defined!');
        }
        
        if ($runUidAlias = $this->getStepRunUidAttributeAlias()) {
            $toSql = $this->getToObject()->getAttribute($runUidAlias)->getDataAddress();
            $insertValues .= ', ' . $toSql;
            $insertCols .= ', [#step_run_uid#]';
            $updates .= ($updates ? ', ' : '') . "[#merge_target#].{$toSql} = [#step_run_uid#]";
        }
        
        $mergeCondition = StringDataType::replacePlaceholders($this->getMergeOnCondition());
        
        if ($incr = $this->getIncrementalWhere()) {
            $mergeCondition .= ' AND ' . $incr;
        }
        
        return <<<SQL

MERGE [#to_object_address#] [#merge_target#] USING [#from_object_address#] [#merge_source#]
    ON {$mergeCondition}
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
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null)
    {
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult), [
            'merge_source' => 'exfs',
            'merge_target' => 'exft'
        ]);
    }
    
    /**
     * 
     * @return string
     */
    protected function getMergeOnCondition() : string
    {
        return $this->mergeOnCondition;
    }
    
    /**
     * 
     * @param string $value
     * @return MsSQLMerge
     */
    protected function setMergeOnCondition(string $value) : MsSQLMerge
    {
        $this->mergeOnCondition = $value;
        return $this;
    }
}