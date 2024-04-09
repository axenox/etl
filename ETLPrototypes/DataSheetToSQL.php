<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Factories\DataSheetFactory;
use axenox\ETL\Common\AbstractETLPrototype;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\DateTimeDataType;
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use exface\Core\Exceptions\UnexpectedValueException;

/**
 * Reads a data sheet from the from-object and generates an SQL query from its rows.
 * 
 * ## Handling empty/NULL values
 * 
 * Data sheets may contain different types of empty values: e.g. empty strings and NULL values. By
 * default all of them are automatically normalized to `NULL` in SQL. However, you can change this
 * behavior using the `sql_empty_values` property.
 * 
 * ## Examples
 * 
 * ### Import an entire data sheet as SQL INSERTs
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
 *      "sql_row_templates": {
 *          "rows": "('[#attr1#]', '[#attr2#]', [#flow_run_uid#])"
 *      }
 *  }
 * 
 * ```
 * 
 * ### Multiple SQL statements for a large data sheet
 * 
 * By default this step will read all data from the data sheet and perform an SQL statement
 * for every set of 500 rows wrapped in a single transaction. If you need more rows per statement,
 * increase `sql_rows_max_per_query` or set it to `null` to place all rows in a single statement.
 * 
 * ```
 *  {
 *      "from_data_sheet": {"columns": []},
 *      "sql": "INSERT INTO [#from_object_address#] (col1, col2, etlRunUID) VALUES [#rows#]",
 *      "sql_rows_max_per_query": 1000,
 *      "sql_row_templates": {"rows": "('[#attr1#]', '[#attr2#]', [#flow_run_uid#])"}
 *  }
 * 
 * ```
 * 
 * ### Paged reading
 * 
 * You can also limit the number of rows to read at a time by setting a `page_size`. The following example
 * will read 1000 rows at a time and put them into 4 SQL statements of 250 rows each.
 * 
 * ```
 *  {
 *      "page_size": 1000,
 *      "from_data_sheet": {"columns":[]},
 *      "sql": "INSERT INTO [#from_object_address#] (col1, col2, etlRunUID) VALUES [#rows#]",
 *      "sql_rows_max_per_query": 250,
 *      "sql_row_templates": {"rows": "('[#attr1#]', '[#attr2#]', [#flow_run_uid#])"}
 *  }
 * 
 * ```
 * 
 * @author andrej.kabachnik
 *
 */
class DataSheetToSQL extends AbstractETLPrototype
{
    const EMPTY_AS_STRING = "''";
    
    const EMPTY_AS_NULL = "NULL";
    
    const EMPTY_AS_IS = null;
    
    private $sourceSheetUxon = null;
    
    private $pageSize = null;
    
    private $incrementalTimeAttributeAlias = null;
    
    private $incrementalTimeOffsetMinutes = 0;
    
    private $baseSheet = null;
    
    private $sqlTpl = null;
    
    private $sqlRowTpls = null;
    
    private $sqlRowsMax = 500;
    
    private $sqlRowDelimiter = "\n,";
    
    private $sqlEmptyValues = self::EMPTY_AS_NULL;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(ETLStepDataInterface $stepData) : \Generator
    {
    	$baseSheet = $this->getFromDataSheet($this->getPlaceholders($stepData));
    	$result = new IncrementalEtlStepResult($stepData->getStepRunUid());
        
        if ($limit = $this->getPageSize()) {
            $baseSheet->setRowsLimit($limit);
        }
        $baseSheet->setAutoCount(false);
        
        $lastResult = $stepData->getLastResult();
        if ($this->isIncrementalByTime()) {
            if ($lastResult instanceof IncrementalEtlStepResult) {
                $lastResultIncrement = $lastResult->getIncrementValue();
            }
            if ($lastResultIncrement !== null && $this->getIncrementalTimeAttributeAlias() !== null) {
                $baseSheet->getFilters()->addConditionFromAttribute(
                	$this->getIncrementalTimeAttribute(), 
                	$lastResultIncrement, 
                	ComparatorDataType::GREATER_THAN_OR_EQUALS);
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
        
        $totalCnt = 0;
        $offset = 0;
        $maxSqlRows = $this->getSqlRowsMaxPerQuery();
        do {
            $fromSheet = $baseSheet->copy();
            $fromSheet->setRowsOffset($offset);
            yield 'Reading ' 
                . ($limit ? 'rows ' . ($offset+1) . ' - ' . ($offset+$limit) : 'all rows') 
                . ($lastResultIncrement !== null ? ' starting from "' . $lastResultIncrement . '"' : '');
            
            $fromSheet->dataRead();
            $fromSheetCnt = $fromSheet->countRows();
            $maxSqlRows = min($fromSheetCnt, $maxSqlRows);
            if ($maxSqlRows > $fromSheetCnt) {
                $sqlTimeout = round($this->getTimeout() / ($fromSheetCnt / $maxSqlRows), 0);
            } else {
                $sqlTimeout = $this->getTimeout();
            }
            if ($fromSheetCnt > 0) {
                $processedCnt = 0;
                while ($processedCnt < $fromSheetCnt) {
                    $sqlRunner = new SQLRunner($this->getName(), $this->getToObject(), $this->getFromObject(), new UxonObject([
                        'sql' => $this->buildSql($fromSheet, $maxSqlRows, $processedCnt)
                    ]));
                    // Each SQL gets a longer timeout
                    $sqlRunner->setTimeout($sqlTimeout);
                    // Do not use own transaction in the SQL runner. Commit here further below
                    // only if all SQLs succeed.
                    $sqlRunner->setWrapInTransaction(false);
                    $processedCnt += $maxSqlRows;
                    yield from $sqlRunner->run($stepData);
                }
            }
            
            $totalCnt += $fromSheetCnt;
            $offset += $limit;
        } while ($limit !== null && $fromSheet->isPaged() && $fromSheetCnt >= $limit);
        
        $transaction->commit();
        
        yield "... mapped {$totalCnt} rows SQL." . PHP_EOL;
        
        return $result->setProcessedRowsCounter($totalCnt);
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
    protected function buildSql(DataSheetInterface $sheet, int $limit, int $offset) : string
    {
        $sql = $this->getSql();
        $sqlNull = $this->getSqlEmptyValue();
        $firstRow = $offset;
        $lastRow = min($limit + $offset, $sheet->countRows());
        foreach ($this->getSqlRowTemplates() as $tplPh => $sqlRowTpl) {
            $rowPhs = StringDataType::findPlaceholders($sqlRowTpl);
            $sqlRows = [];
            for ($i = $firstRow; $i < $lastRow; $i++) {
                $row = $sheet->getRow($i);
                $rowPhValues = [];
                $rowNulls = [];
                foreach ($rowPhs as $ph) {
                    $colExists = array_key_exists($ph, $row);
                    $rowPhValue = $row[$ph];
                    switch (true) {
                        // Leave the placeholder as-is if there is no matching data column
                        case $colExists === false:
                            $rowPhValues[$ph] = '[#' . $ph . '#]';
                            break;
                        // Normalize empty existing data values (column exists, but value empty)
                        case $rowPhValue === '' || $rowPhValue === null || $rowPhValue === EXF_LOGICAL_NULL:
                            // If they need to be normalized to a specific value, use that value
                            // Otherwise turn empty strings to `''` and all other values to `NULL`.
                            // In both cases, the custom replacer below will take care of removing
                            // any surrounding quotes from the template
                            if ($sqlNull !== null) {
                                $rowNulls[$ph] = $sqlNull;
                            } else {
                                $rowNulls[$ph] = $rowPhValue === '' ? "''" : "NULL";
                            }
                            break;
                        // Use existing non-empty row values directly
                        default:
                            // FIXME escape strings here!
                            $rowPhValues[$ph] = $rowPhValue;
                            break;
                    }
                }

                // Replace placeholders with emtpy values by their normalized value BEFORE
                // processing the other placeholders. This is important, because `replacePlaceholders()`
                // will de-facto normalize empty values to empty strings (because PHPs `null` will 
                // result in an empty string when stringified). Thus `'[#mycol#]'` will become `''`
                // and not `NULL`. The replacer below will also get rid of the quotation marks, convertig
                // `'[#mycol#]'` to `NULL` correctly.
                $sqlRow = $sqlRowTpl;
                foreach ($rowNulls as $ph => $val){
                    // If the value is really NULL, remove the quotation marks
                    $sqlRow = preg_replace("/('?\[#{$ph}#]'?)/", $val, $sqlRow);
                }

                // fill placeholders with values
                $sqlRows[] = StringDataType::replacePlaceholders($sqlRow, $rowPhValues);
            }

            $sql = StringDataType::replacePlaceholder($sql, $tplPh, implode($this->getSqlRowDelimiter(), $sqlRows));
        }
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
     * @uxon-template INSERT INTO [#to_object_address#] (col1, col2, ETLFlowRunUID) VALUES [#rows#]
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
     * @return string[]
     */
    protected function getSqlRowTemplates() : array
    {
        return $this->sqlRowTpls;
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
     * @uxon-property sql_row_templates
     * @uxon-type object
     * @uxon-template {"rows":"('[#attr1#]', '[#attr1#]', [#flow_run_uid#])"}
     * 
     * @param string $arrayOfSqlStrings
     * @return DataSheetToSQL
     */
    protected function setSqlRowTemplates(UxonObject $arrayOfSqlStrings) : DataSheetToSQL
    {
        $this->sqlRowTpls = $arrayOfSqlStrings->toArray();
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
    
    /**
     * 
     * @return int|NULL
     */
    protected function getSqlRowsMaxPerQuery() : ?int
    {
        return $this->sqlRowsMax;
    }
    
    /**
     * Maximum number of rows to put into a single SQL query.
     * 
     * Larger data sheets will produce multiple SQL statement
     * 
     * @uxon-property sql_rows_max_per_query
     * @uxon-type number
     * @uxon-default 500
     * 
     * @param int|NULL $value
     * @return DataSheetToSQL
     */
    protected function setSqlRowsMaxPerQuery(?int $value) : DataSheetToSQL
    {
        $this->sqlRowsMax = $value;
        return $this;
    }
    
    /**
     * 
     * @return string|NULL
     */
    protected function getSqlEmptyValue() : ?string
    {
        return $this->sqlEmptyValues;
    }
    
    /**
     * What to do with empty values in the data sheet?
     * 
     * By default, this type of flow step will normalize all empty values to SQL NULLs (`as_null`). 
     * However, you can change this behavior to either normalize them to empty strings (`as_string`)
     * or leave them `as_is`.
     * 
     * @uxon-property sql_empty_values
     * @uxon-type [as_null,as_string,as_is]
     * @uxon-default as_null
     * 
     * @param string $value
     * @return DataSheetToSQL
     */
    public function setSqlEmptyValues(string $value) : DataSheetToSQL
    {
        $const = DataSheetToSQL::class . '::EMPTY_' . strtoupper($value);
        if (! defined($const)) {
            throw new UnexpectedValueException('Invalid value "' . $value . '" for property "sql_empty_values" in data flow step "' . $this->getName() . '"');
        }
        $value = constant($const);
        $this->sqlEmptyValues = $value;
        return $this;
    }
}