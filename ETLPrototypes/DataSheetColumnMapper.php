<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\CommonLogic\UxonObject;
use axenox\ETL\Interfaces\Generator;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetMapperInterface;
use exface\Core\Factories\DataSheetMapperFactory;
use exface\Core\Factories\BehaviorFactory;
use exface\Core\Behaviors\PreventDuplicatesBehavior;
use axenox\ETL\Common\AbstractETLPrototype;

class DataSheetColumnMapper extends AbstractETLPrototype
{
    private $mapperUxon = null;
    
    private $sourceSheetUxon = null;
    
    private $updateIfMatchingAttributeAliases = [];
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, string $previousStepRunUid = null) : \Generator
    {
        $fromSheet = $this->getFromDataSheet();
        $mapper = $this->getMapper();
        
        $toSheet = $mapper->map($fromSheet, true);
        $toSheet->getColumns()
        ->addFromAttribute($this->getToObject()->getAttribute($this->getStepRunUidAttributeAlias()))
        ->setValueOnAllRows($stepRunUid);
        
        $this->addDuplicatePreventingBehavior($this->getToObject());
        $cnt = $toSheet->dataCreate();
        
        yield 'DataSheetColumnMapper: created/updated ' . $cnt . ' rows' . PHP_EOL;
    }

    public function validate(): \Generator
    {
        yield from [];
    }
    
    protected function addDuplicatePreventingBehavior(MetaObjectInterface $object)
    {
        $behavior = BehaviorFactory::createFromUxon($object, PreventDuplicatesBehavior::class, new UxonObject([
            'compare_attributes' => $this->getUpdateIfMatchingAttributeAliases(),
            'on_duplicate_multi_row' => PreventDuplicatesBehavior::ON_DUPLICATE_UPDATE,
            'on_duplicate_single_row' => PreventDuplicatesBehavior::ON_DUPLICATE_UPDATE
        ]));
        $object->getBehaviors()->add($behavior);
        return;
    }
    
    /**
     * 
     * @return string[]
     */
    protected function getUpdateIfMatchingAttributeAliases() : array
    {
        return $this->updateIfMatchingAttributeAliases;
    }
    
    /**
     * The attributes to compare when searching for existing data rows
     *
     * @uxon-property update_if_match_on_attributes
     * @uxon-type metamodel:attribute[]
     * @uxon-template [""]
     * 
     * @param UxonObject $uxon
     * @return DataSheetColumnMapper
     */
    protected function setUpdateIfMatchingAttributes(UxonObject $uxon) : DataSheetColumnMapper
    {
        $this->updateIfMatchingAttributeAliases = $uxon->toArray();
        return $this;
    }

    /**
     * 
     * @param MetaObjectInterface $fromObject
     * @return DataSheetInterface
     */
    protected function getFromDataSheet() : DataSheetInterface
    {
        $ds = DataSheetFactory::createFromObject($this->getFromObject());
        if ($this->sourceSheetUxon && ! $this->sourceSheetUxon->isEmpty()) {
            $ds->importUxonObject($this->sourceSheetUxon);
        }
        return $ds;
    }
    
    /**
     * The data sheet to read the from-object data.
     * 
     * @uxon-property from_data_sheet
     * @uxon-type \exface\Core\CommonLogic\DataSheets\DataSheet
     * @uxon-template {"filters":{"operator": "AND","conditions":[{"expression": "","comparator": "=","value": ""}]},"sorters": [{"attribute_alias": "","direction": "ASC"}]}
     * 
     * @param UxonObject $uxon
     * @return DataSheetColumnMapper
     */
    protected function setFromDataSheet(UxonObject $uxon) : DataSheetColumnMapper
    {
        $this->sourceSheetUxon = $uxon;
        return $this;
    }
    
    /**
     * 
     * @param MetaObjectInterface $fromObject
     * @param MetaObjectInterface $toObject
     * @return DataSheetMapperInterface
     */
    protected function getMapper() : DataSheetMapperInterface
    {
        if (! $this->mapperUxon || $this->mapperUxon->isEmpty()) {
            // TODO throw error
        }
        return DataSheetMapperFactory::createFromUxon($this->getWorkbench(), $this->mapperUxon, $this->getFromObject(), $this->getToObject()); 
    }
    
    /**
     * The mapper to apply to the `from_data_sheet` to transform it into to-object data
     * 
     * @uxon-property mapper
     * @uxon-type \exface\Core\CommonLogic\DataSheets\DataSheetMapper
     * @uxon-template {"column_to_column_mappings": [{"from": "", "to": ""}]}
     * @uxon-required true
     * 
     * @param UxonObject $uxon
     * @return DataSheetColumnMapper
     */
    protected function setMapper(UxonObject $uxon) : DataSheetColumnMapper
    {
        $this->mapperUxon = $uxon;
        return $this;
    }
}