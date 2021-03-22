<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetMapperInterface;
use exface\Core\Factories\DataSheetMapperFactory;
use exface\Core\Factories\BehaviorFactory;
use exface\Core\Behaviors\PreventDuplicatesBehavior;
use axenox\ETL\Common\AbstractETLPrototype;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\DateTimeDataType;

class DataSheetTransfer extends AbstractETLPrototype
{
    private $mapperUxon = null;
    
    private $sourceSheetUxon = null;
    
    private $updateIfMatchingAttributeAliases = [];
    
    private $pageSize = null;
    
    private $incrementalTimeAttributeAlias = null;
    
    private $incrementalTimeOffsetMinutes = 0;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, string $previousStepRunUid = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $baseSheet = $this->getFromDataSheet();
        $mapper = $this->getMapper();
        $result = new IncrementalEtlStepResult($stepRunUid);
        
        $this->addDuplicatePreventingBehavior($this->getToObject());
        
        if ($limit = $this->getPageSize()) {
            $baseSheet->setRowsLimit($limit);
        }
        $baseSheet->setAutoCount(false);
        
        if ($this->isIncrementalByTime()) {
            if ($lastResult instanceof IncrementalEtlStepResult) {
                $lastResultIncrement = $lastResult->getIncrementValue();
            }
            if ($lastResultIncrement !== null) {
                $baseSheet->getFilters()->addConditionFromAttribute($this->getIncrementalTimeAttribute(), $lastResultIncrement, ComparatorDataType::GREATER_THAN_OR_EQUALS);
            }
            $result->setIncrementValue(DateTimeDataType::formatDateNormalized($this->getIncrementalTimeCurrentValue()));
        }
        
        $transaction = $this->getWorkbench()->data()->startTransaction();
        
        $cnt = 0;
        $offset = 0;
        try {
            do {
                $fromSheet = $baseSheet->copy();
                $fromSheet->setRowsOffset($offset);
                yield 'Reading ' . ($limit ? 'rows ' . ($offset+1) . ' - ' . ($offset+$limit) . '...' : 'all rows...') . PHP_EOL;
                
                $toSheet = $mapper->map($fromSheet, true);
                $toSheet->getColumns()
                    ->addFromAttribute($this->getToObject()->getAttribute($this->getStepRunUidAttributeAlias()))
                    ->setValueOnAllRows($stepRunUid);
                $toSheet->dataCreate(false, $transaction);
                
                $cnt += $fromSheet->countRows();
                $offset = $offset + $limit;
                /*if ($cnt > 2000) {
                    throw new RuntimeException('Testing transactions');
                }*/
            } while ($limit !== null && $fromSheet->isPaged() && $fromSheet->countRows() >= $limit);
        } catch (\Throwable $e) {
            $transaction->rollback();
            throw $e;
        }
        $transaction->commit();
        
        yield ' processed ' . $cnt . ' rows in total' . PHP_EOL;
        
        return $result->setProcessedRowsCounter($cnt);
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
     * @uxon-property update_if_matching_attributes
     * @uxon-type metamodel:attribute[]
     * @uxon-template [""]
     * 
     * @param UxonObject $uxon
     * @return DataSheetTransfer
     */
    protected function setUpdateIfMatchingAttributes(UxonObject $uxon) : DataSheetTransfer
    {
        $this->updateIfMatchingAttributeAliases = $uxon->toArray();
        return $this;
    }

    /**
     * 
     * @param MetaObjectInterface $fromObject
     * @return DataSheetInterface
     */
    protected function getFromDataSheet(array $placeholders = []) : DataSheetInterface
    {
        $ds = DataSheetFactory::createFromObject($this->getFromObject());
        if ($this->sourceSheetUxon && ! $this->sourceSheetUxon->isEmpty()) {
            $json = $this->sourceSheetUxon->toJson();
            StringDataType::replacePlaceholders($json, $placeholders);
            $ds->importUxonObject(UxonObject::fromJson($json));
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
     * @return DataSheetTransfer
     */
    protected function setFromDataSheet(UxonObject $uxon) : DataSheetTransfer
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
     * @return DataSheetTransfer
     */
    protected function setMapper(UxonObject $uxon) : DataSheetTransfer
    {
        $this->mapperUxon = $uxon;
        return $this;
    }
    
    protected function getPageSize() : ?int
    {
        return $this->pageSize;
    }
    
    /**
     * Number of rows to process at once - no limit if NULL.
     * 
     * @uxon-property page_size
     * @uxon-type integers
     * 
     * @param int $numberOfRows
     * @return DataSheetTransfer
     */
    protected function setPageSize(int $numberOfRows) : DataSheetTransfer
    {
        $this->pageSize = $numberOfRows;
        return $this;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        return new IncrementalEtlStepResult($stepRunUid, $resultData);
    }
    
    /**
     * 
     * @return string|NULL
     */
    protected function getIncrementalTimeAttributeAlias() : ?string
    {
        return $this->incrementalTimeAttributeAlias;
    }
    
    /**
     * 
     * @return MetaAttributeInterface
     */
    protected function getIncrementalTimeAttribute() : MetaAttributeInterface
    {
        return $this->getFromObject()->getAttribute($this->getIncrementalTimeAttributeAlias());
    }
    
    /**
     * Alias of the from-object attribute holding last change time to be used for incremental loading.
     * 
     * @uxon-property incremental_time_attribute_alias
     * @uxon-type metamodel:attribute
     * 
     * @param string $value
     * @return DataSheetTransfer
     */
    protected function setIncrementalTimeAttributeAlias(string $value) : DataSheetTransfer
    {
        $this->incrementalTimeAttributeAlias = $value;
        return $this;
    }
    
    public function isIncrementalByTime() : bool
    {
        return $this->incrementalTimeAttributeAlias !== null;
    }
    
    /**
     * Number of minutes to add/subtract from the current time when initializing the increment value
     * 
     * @uxon-property incremental_time_offset_in_minutes
     * @uxon-type integer
     * 
     * @return int
     */
    protected function getIncrementalTimeOffsetInMinutes() : int
    {
        return $this->incrementalTimeOffsetMinutes;
    }
    
    protected function setIncrementalTimeOffsetInMinutes(int $value) : DataSheetTransfer
    {
        $this->incrementalTimeOffsetMinutes = $value;
        return $this;
    }
    
    protected function getIncrementalTimeCurrentValue() : \DateTime
    {
        $now = new \DateTime();
        $offset = $this->getIncrementalTimeOffsetInMinutes();
        if ($offset < 0) {
            $now->sub(new \DateInterval('PT' . abs($offset) . 'M'));
        } elseif ($offset > 0) {
            $now->add(new \DateInterval('PT' . abs($offset) . 'M'));
        }
        return $now;
    }
}