<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractETLPrototype;
use exface\Core\Interfaces\DataSources\SqlDataConnectorInterface;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\CommonLogic\DataQueries\SqlDataQuery;
use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Common\UxonEtlStepResult;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use exface\Core\Widgets\DebugMessage;

/**
 * Runs any SQL script in the data source of the to-object
 * 
 * The script ist to be defined in the `sql` property. This step requires the from- and to-objects
 * to have the same data source and that must have an SQL connection. It is also possible to leave
 * the from-object empty.
 * 
 * If this step should be an incremental one, place the SQL to generate the current increment value
 * in `sql_to_get_current_increment_value` - e.g. `SELECT NOW();` or similar. The result of this
 * script will be saved in the step result data and will be available to the next run of this step
 * via `[#last_run_increment_value#]` placeholder.
 * 
 * The `sql` may include the following placeholders:
 * 
 * - `[#from_object_address#]`
 * - `[#to_object_address#]`
 * - `[#step_run_uid#]`
 * - `[#last_run_uid#]`
 * - `[#last_run_increment_value#]`
 * 
 * If it is an incremental step, additional placeholders are available:
 * 
 * - `[#current_increment_value#]`
 * 
 * **NOTE:** most SQL connections do not support explicit transaction handling in SQL scripts.
 * Instead use `wrap_in_transaction` to force/omit the commit at the end of this step.
 * 
 * **HINT:** There are also preconfigured ETL step prototypes for commonly used SQL statements
 * 
 * - `MySQLInsertOnDuplicateKeyUpdate`,
 * - `MySQLReplace`
 * - `MySQLDateDimensionGenerator`,
 * - `MsSQLMerge`
 * - `SQLSelectValidator`
 * 
 * @author andrej.kabachnik
 *
 */
class SQLRunner extends AbstractETLPrototype
{
    private $sql = null;
    
    private $sqlToGetCurrentIncrementValue = null;
    
    private $wrapInTransaction = true;
    
    private $query = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, ETLStepResultInterface $previousStepResult = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $sql = $this->getSql();
        $connection = $this->getSqlConnection();
        $result = new IncrementalEtlStepResult($stepRunUid);
        
        if ($this->isWrappedInTransaction()) {
            $connection->transactionStart();
        }
        
        $phs = $this->getPlaceholders($stepRunUid, $lastResult);
        // Handle incremental logic
        if ($this->isIncremental()) {
            if (! array_key_exists('last_run_increment_value', $phs)) {
                $phs['last_run_increment_value'] = '';
            }
            $incrSql = $this->getSqlToGetCurrentIncrementValue();
            if ($incrSql === null || $incrSql === '') {
                throw new RuntimeException('Cannot get current increment value for ETL step "' . $this->getName() . '": please specify `sql_to_get_current_increment_value` in the steps configuration!');
            }
            $incrSql = StringDataType::replacePlaceholders($incrSql, $phs);
            $incrRow = $connection->runSql($incrSql)->getResultArray()[0] ?? null;
            if (! is_array($incrRow) || count($incrRow) !== 1) {
                throw new RuntimeException('Cannot get current increment value for ETL step"' . $this->getName() . '": the SQL to get the value does not return a single value!');
            }
            $currentIncrement = reset($incrRow);
            $phs['current_increment_value'] = $currentIncrement;
            $result->setIncrementValue($currentIncrement);
        }
        
        $sql = StringDataType::replacePlaceholders($sql, $phs, true, true);
        
        // Execute the main statement
        $query = new SqlDataQuery();
        $query->setSql($sql);
        $query->forceMultipleStatements(true);
        $this->query = $query;
        
        $this->getWorkbench()->eventManager()->dispatch(new OnBeforeETLStepRun($this));
        
        $query = $connection->query($query);
        $cnt = $this->countAffectedRows($query);
        $query->freeResult();
        
        if ($this->isWrappedInTransaction()) {
            $connection->transactionCommit();
        }
        
        yield 'SQL query successful (' . $cnt . ' affected rows' . ($phs['last_run_increment_value'] ? ', starting with ' . $phs['last_run_increment_value'] : '') . ')' . PHP_EOL;
        
        if ($cnt !== null) {
            $result->setProcessedRowsCounter($cnt);
        }
        return $result;
    }
    
    /**
     * 
     * @param SqlDataQuery $query
     * @return int|NULL
     */
    protected function countAffectedRows(SqlDataQuery $query) : ?int
    {
        return $query->countAffectedRows();
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::validate()
     */
    public function validate(): \Generator
    {
        yield from [];
    }
    
    /**
     * 
     * @return string
     */
    protected function getSql() : string
    {
        return $this->sql ?? '';
    }
    
    /**
     * The SQL to run (supports multiple statements!)
     * 
     * Available placeholders:
     * 
     * - `[#from_object_address#]`
     * - `[#to_object_address#]`
     * - `[#step_run_uid#]`
     * - `[#last_run_uid#]`
     * - `[#last_run_increment_value#]`
     * 
     * If it is an incremental step, additional placeholders are available:
     * 
     * - `[#current_increment_value#]`
     * 
     * @uxon-property sql
     * @uxon-type string
     * 
     * @param string $value
     * @return SQLRunner
     */
    protected function setSql(string $value) : SQLRunner
    {
        $this->sql = $value;
        return $this;
    }
    
    /**
     * 
     * @return SqlDataConnectorInterface
     */
    protected function getSqlConnection() : SqlDataConnectorInterface
    {
        return $this->getToObject()->getDataConnection();
    }
    
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult),[
            'from_object_address' => $this->getFromObject()->getDataAddress(),
            'to_object_address' => $this->getToObject()->getDataAddress()
        ]);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        if ($resultData !== null && stripos($resultData, 'increment_') !== false) {
            return new IncrementalEtlStepResult($stepRunUid, $resultData);
        } else {
            return new UxonEtlStepResult($stepRunUid, $resultData);
        }
    }
    
    protected function isWrappedInTransaction() : bool
    {
        return $this->wrapInTransaction;
    }
    
    /**
     * Set to FALSE to skip starting/committing a transaction before/after the SQL query.
     * 
     * Disabling the transaction may be usefull if transaction handling is done by the query
     * itself (if supported by the database).
     * 
     * @uxon-property wrap_in_transaction
     * @uxon-type boolean
     * @uxon-default true
     * 
     * @param bool $value
     * @return SQLRunner
     */
    public function setWrapInTransaction(bool $value) : SQLRunner
    {
        $this->wrapInTransaction = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isIncremental()
     */
    public function isIncremental() : bool
    {
        return $this->sqlToGetCurrentIncrementValue !== null || stripos($this->getSql(), '[#last_run_') !== false;
    }
    
    /**
     * 
     * @return string|NULL
     */
    protected function getSqlToGetCurrentIncrementValue() : ?string
    {
        return $this->sqlToGetCurrentIncrementValue;
    }
    
    /**
     * An SQL statement to read/generate the current increment value (makes the step incremental!).
     * 
     * E.g. `SELECT NOW();` - anything data source will return a single result for.
     * 
     * Note: if this property is set, the step is concidered to be incremental.
     * 
     * @uxon-property sql_to_get_current_increment_value
     * @uxon-type string
     * 
     * @param string $value
     * @return SQLRunner
     */
    protected function setSqlToGetCurrentIncrementValue(string $value) : SQLRunner
    {
        $this->sqlToGetCurrentIncrementValue = $value;
        return $this;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        if ($this->query !== null) {
            $debug_widget = $this->query->createDebugWidget($debug_widget);
        }
        return $debug_widget;
    }
}