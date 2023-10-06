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
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use exface\Core\Exceptions\UxonParserError;
use exface\Core\DataTypes\PhpClassDataType;

/**
 * Reads a data sheet from the from-object and generates an SQL query from its rows.
 * 
 * Example
 * 
 * ```
 *  {
 *      "from_data_sheet": {
 *          "columns": [
 *              {"attribute_alias": "attr1"},
 *              {"attribute_alias": "attr2"}
 *          ]
 *      },
 *      "sql": "INSERT INTO [#from_object_address#] (col1, col2, etlRunUID) VALUES [#rows#]",
 *      "sql_row_template": "('[#attr1#]', '[#attr2#]', [#flow_run_uid#])"
 *  }
 * 
 * ```
 * 
 * @author andrej.kabachnik
 *
 */
class DataSheetToSQL extends AbstractETLPrototype
{
    private $sourceSheetUxon = null;
    
    private $pageSize = null;
    
    private $incrementalTimeAttributeAlias = null;
    
    private $incrementalTimeOffsetMinutes = 0;
    
    private $baseSheet = null;
    
    private $sqlTpl = null;
    
    private $sqlRowTpl = null;
    
    private $sqlRowDelimiter = "\n,";
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $flowRunUid, string $stepRunUid, ETLStepResultInterface $previousStepResult = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $baseSheet = $this->getFromDataSheet($this->getPlaceholders($flowRunUid, $stepRunUid, $lastResult));
        $result = new IncrementalEtlStepResult($stepRunUid);
        
        if ($limit = $this->getPageSize()) {
            $baseSheet->setRowsLimit($limit);
        }
        $baseSheet->setAutoCount(false);
        
        if ($this->isIncrementalByTime()) {
            if ($lastResult instanceof IncrementalEtlStepResult) {
                $lastResultIncrement = $lastResult->getIncrementValue();
            }
            if ($lastResultIncrement !== null && $this->getIncrementalTimeAttributeAlias() !== null) {
                $baseSheet->getFilters()->addConditionFromAttribute($this->getIncrementalTimeAttribute(), $lastResultIncrement, ComparatorDataType::GREATER_THAN_OR_EQUALS);
            }
            $result->setIncrementValue(DateTimeDataType::formatDateNormalized($this->getIncrementalTimeCurrentValue()));
        }
        
        $this->baseSheet = $baseSheet;
        
        $this->getWorkbench()->eventManager()->dispatch(new OnBeforeETLStepRun($this));
        
        $transaction = $this->getWorkbench()->data()->startTransaction();
        
        if ($lastResult !== null) {
            $lastResultForSqlRunner = SQLRunner::parseResult($lastResult->getStepRunUid(), $lastResult->exportUxonObject()->toJson());
        } else {
            $lastResultForSqlRunner = null;
        }
        
        $cntFrom = 0;
        $offset = 0;
        do {
            $fromSheet = $baseSheet->copy();
            $fromSheet->setRowsOffset($offset);
            yield 'Reading ' 
                . ($limit ? 'rows ' . ($offset+1) . ' - ' . ($offset+$limit) : 'all rows') 
                . ($lastResultIncrement !== null ? ' starting from "' . $lastResultIncrement . '"' : '');
            
            $fromSheet->dataRead();
            
            if ($fromSheet->countRows() > 0) {
                $sqlRunner = new SQLRunner($this->getName(), $this->getToObject(), $this->getFromObject(), new UxonObject([
                    'sql' => $this->buildSql($fromSheet)
                ]));
                $sqlRunner->setTimeout($this->getTimeout());
                yield from $sqlRunner->run($flowRunUid, $stepRunUid, $previousStepResult, $lastResultForSqlRunner);
            }
            
            $cntFrom += $fromSheet->countRows();
            $offset = $offset + $limit;
        } while ($limit !== null && $fromSheet->isPaged() && $fromSheet->countRows() >= $limit);
        
        $transaction->commit();
        
        yield "... mapped {$cntFrom} rows SQL." . PHP_EOL;
        
        return $result->setProcessedRowsCounter($cntFrom);
    }

    public function validate(): \Generator
    {
        yield from [];
    }
    
    /**
     * 
     * @param DataSheetInterface $sheet
     * @return string
     */
    protected function buildSql(DataSheetInterface $sheet) : string
    {
        $sql = $this->getSql();
        $sqlRowTpl = $this->getSqlRowTemplate();
        $rowPhs = StringDataType::findPlaceholders($sqlRowTpl);
        $sqlRows = [];
        foreach ($sheet->getRows() as $row) {
            $rowPhVals = [];
            foreach ($rowPhs as $ph) {
                if (array_key_exists($ph, $row)) {
                    $rowPhVals[$ph] = $row[$ph];
                } else {
                    $rowPhVals[$ph] = '[#' . $ph . '#]';
                }
            }
            $sqlRows[] = StringDataType::replacePlaceholders($sqlRowTpl, $rowPhVals);
        }
        $sql = str_replace('[#rows#]', implode($this->getSqlRowDelimiter(), $sqlRows), $sql);
        return $sql;
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
            $json = StringDataType::replacePlaceholders($json, $placeholders);
            $ds->importUxonObject(UxonObject::fromJson($json));
        }
        return $ds;
    }
    
    /**
     * Customize the data sheet to read the from-object data.
     * 
     * Typical things to do are adding `filters`, `sorters` or even `aggregate_by_attribute_alias`. 
     * Placeholders can be used anywhere in the data sheet model:
     * 
     * - `step_run_uid`
     * - `flow_run_uid`
     * - `last_run_uid`
     * - `last_run_increment_value`
     * - `laxt_run_xxx` - any property of the result data of the last run of this step (replace `xxx`
     * with the property name like `last_run_increment_value` for `increment_value` from the last runs
     * result)
     * 
     * @uxon-property from_data_sheet
     * @uxon-type \exface\Core\CommonLogic\DataSheets\DataSheet
     * @uxon-template {"filters":{"operator": "AND","conditions":[{"expression": "","comparator": "=","value": ""}]},"sorters": [{"attribute_alias": "","direction": "ASC"}]}
     * 
     * @param UxonObject $uxon
     * @return DataSheetTransfer
     */
    protected function setFromDataSheet(UxonObject $uxon) : DataSheetToSQL
    {
        $this->sourceSheetUxon = $uxon;
        return $this;
    }
    
    protected function getPageSize() : ?int
    {
        return $this->pageSize;
    }
    
    /**
     * Number of rows to process at once - no limit if set to NULL.
     * 
     * The step will make as many requests to the from-objects data source as needed to read
     * all data by reading `page_size` rows at a time.
     * 
     * @uxon-property page_size
     * @uxon-type integers
     * 
     * @param int $numberOfRows
     * @return DataSheetTransfer
     */
    protected function setPageSize(int $numberOfRows) : DataSheetToSQL
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
     * If set, a filter over this attribute will be added to the `from_data_sheet` automatically.
     * Alternatively you can add the incremental filter(s) explicitly using the placeholder 
     * `[#last_run_increment_value#]`.
     * 
     * @uxon-property incremental_time_attribute_alias
     * @uxon-type metamodel:attribute
     * 
     * @param string $value
     * @return DataSheetTransfer
     */
    protected function setIncrementalTimeAttributeAlias(string $value) : DataSheetToSQL
    {
        $this->incrementalTimeAttributeAlias = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isIncremental()
     */
    public function isIncremental() : bool
    {
        return $this->isIncrementalByTime();
    }
    
    /**
     * 
     * @return bool
     */
    protected function isIncrementalByTime() : bool
    {
        if ($this->incrementalTimeAttributeAlias !== null) {
            return true;
        }
        if ($this->sourceSheetUxon && stripos($this->sourceSheetUxon->toJson(), '[#last_run_') !== false) {
            return true;
        }
        
        return false;
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
    
    protected function setIncrementalTimeOffsetInMinutes(int $value) : DataSheetToSQL
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
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        if ($this->baseSheet !== null) {
            $debug_widget = $this->baseSheet->createDebugWidget($debug_widget);
        }
        return $debug_widget;
    }
    
    /**
     * 
     * @return string
     */
    protected function getSql() : string
    {
        return $this->sqlTpl;
    }
    
    /**
     * The SQL statement to be performed for every data sheet being read
     * 
     * @uxon-property sql
     * @uxon-type string
     * @uxon-required true
     * 
     * @param string $value
     * @return DataSheetToSQL
     */
    protected function setSql(string $value) : DataSheetToSQL
    {
        $this->sqlTpl = $value;
        return $this;
    }
    
    /**
     * 
     * @return string
     */
    protected function getSqlRowTemplate() : string
    {
        return $this->sqlRowTpl;
    }
    
    /**
     * An SQL template for each row of data sheets being read.
     * 
     * Use column names as placeholders: e.g. `[#my_attribute#]` will be replaced by the value of the
     * data column `my_attribute`. 
     * 
     * In most cases, attribute aliases can be used here directlycolumn names are based on them. If this
     * does not work, look into the debug output of this step to find all the column names of the
     * data sheet.
     * 
     * @uxon-property sql_row_template
     * @uxon-type string
     * 
     * @param string $value
     * @return DataSheetToSQL
     */
    protected function setSqlRowTemplate(string $value) : DataSheetToSQL
    {
        $this->sqlRowTpl = $value;
        return $this;
    }
    
    /**
     * 
     * @return string
     */
    protected function getSqlRowDelimiter() : string
    {
        return $this->sqlRowDelimiter;
    }
    
    /**
     * Delimiter to use between each row template - `\n,` by default
     * 
     * @uxon-property sql_row_delimiter
     * @uxon-type string
     * @uxon-default \n,
     * 
     * @param string $value
     * @return DataSheetToSQL
     */
    protected function setSqlRowDelimiter(string $value) : DataSheetToSQL
    {
        $this->sqlRowDelimiter = $value;
        return $this;
    }
}