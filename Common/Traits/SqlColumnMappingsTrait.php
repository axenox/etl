<?php
namespace axenox\ETL\Common\Traits;

use axenox\ETL\Interfaces\ETLStepInterface;
use axenox\ETL\Common\SqlColumnMapping;
use exface\Core\Exceptions\RuntimeException;
use exface\Core\CommonLogic\UxonObject;

trait SqlColumnMappingsTrait
{
    private $columnMappings = [];
    
    /**
     *
     * @return SqlColumnMapping[]
     */
    protected function getColumnMappings() : array
    {
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
     * @return ETLStepInterface
     */
    public function setColumnMappings(UxonObject $arrayOfMappings) : ETLStepInterface
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
}