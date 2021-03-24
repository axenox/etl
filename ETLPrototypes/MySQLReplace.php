<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\UxonObject;
use axenox\ETL\Common\SqlColumnMapping;
use exface\Core\Exceptions\RuntimeException;

class MySQLReplace extends SQLRunner
{    
    private $columnMappings = [];
    
    private $incrementalWhere = null;
    
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
            $targetCols = $targetCols . ($targetCols ? ', ' : '') . $map->getToSql();
            $sourceCols = $sourceCols . ($sourceCols ? ', ' : '') . $map->getFromSql();
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
    
    /**
     * 
     * @return SqlColumnMapping[]
     */
    public function getColumnMappings() : array
    {
        if (empty($this->columnMappings)) {
            $toObj = $this->getToObject();
            $fromObj = $this->getFromObject();
            foreach ($fromObj->getAttributes() as $attr) {
                if ($toObj->hasAttribute($attr->getAlias())) {
                    $this->columnMappings[] = new SqlColumnMapping($fromObj, $toObj, new UxonObject([
                        'from' => $attr->getAlias(),
                        'to' => $attr->getAlias()
                    ]));
                }
            }
        }
        return $this->columnMappings;
    }
    
    /**
     * Attribute aliases or SQL statements to map `from` and `to`.
     * 
     * The `from` and `to` of every mapping can be an alias of the respective meta object
     * or an SQL snippet (possibly including ETL step placeholders).
     * 
     * @uxon-property column_mappings
     * @uxon-type \axenox\ETL\Common\SqlColumnMapping[]
     * @uxon-template [{"from": "", "to": ""}]
     * 
     * @param UxonObject $arrayOfMappings
     * @throws RuntimeException
     * @return MySQLReplace
     */
    public function setColumnMappings(UxonObject $arrayOfMappings) : MySQLReplace
    {
        $fromObj = $this->getFromObject();
        $toObj = $this->getToObject();
        if ($fromObj === null) {
            throw new RuntimeException('Cannot use `column_mappings` in ETL step "' . $this->getName() . '" without an from-object!');
        }
        foreach ($arrayOfMappings->getPropertiesAll() as $uxon) {
            $this->columnMappings[] = new SqlColumnMapping($fromObj, $toObj, $uxon);
        }
        return $this;
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
    protected function setIncrementalWhere(string $value) : MySQLReplace
    {
        $this->incrementalWhere = $value;
        return $this;
    }
}