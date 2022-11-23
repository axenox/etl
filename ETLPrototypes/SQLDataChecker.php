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
use exface\Core\CommonLogic\UxonObject;
use axenox\ETL\Common\SqlDataCheck;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;
use exface\Core\Factories\ActionFactory;
use exface\Core\Factories\TaskFactory;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\CommonLogic\DataSheets\DataColumn;

/**
 * Runs one or more SQL SELECTs to check for issues in the data, producing errors if the SELECTs return at least one row.
 * 
 * All checks are performed on each step run - regardless of whether some of them hit or not. However,
 * if at leas one critical check with `stop_flow_on_hit` (see below) has a hit, the step will result
 * in an error in the end. This way, it will block the flow, but the log will include information about
 * all errors found, not just the one that triggered the stop. 
 * 
 * ## Checks
 * 
 * Each of the `checks` has 
 * 
 * - `sql` script (possible multiple statements) - this script is expected to return some rows if the
 * examined data is NOT OK. 
 * - `message_text`, which will be displayed if the check hits
 * - `stop_flow_on_hit` flag to control if the check is critical
 * 
 * The `sql` may include the following placeholders:
 * 
 * - `[#from_object_address#]`
 * - `[#to_object_address#]`
 * - `[#flow_run_uid#]`
 * - `[#step_run_uid#]`
 * - `[#last_run_uid#]`
 * - `[#last_run_increment_value#]`
 * 
 * If it is an incremental step, additional placeholders are available:
 * 
 * - `[#current_increment_value#]`
 * 
 * ## Errors and messages
 * 
 * Each hit produces as many messages as rows returned by its SQL. The message template is defined
 * in `message_text` and can contain placeholders:
 * 
 * - The global placeholders listed above
 * - Values returned by the checks SELECT statement - e.g. `SELECT myCol AS cnt FROM t` would provide 
 * the placeholder `[#cnt#]` for each row.
 * 
 * The granularity of the errors can easily be controlled using `GROUP BY` clauses in your SQL to 
 * decrease the number of error rows (= messages).
 * 
 * ## Result action to save messages
 * 
 * If you want, you can configure a `result_action` to deal with the resulting messages: e.g. save or send 
 * them somewhare. This action will get an input data sheet based on the object `exface.Core.DUMMY`, 
 * that will contain all the rows returned by the checks. Using an `input_mapper` for the action, you
 * can deal with error data very flexibly - see examples below.
 * 
 * ## Examples
 * 
 * ### Check for empty SQL columns and save results
 * 
 * ```
 *  {
 *      "checks": [{
 *          "message_text": "Autor nicht angegeben",
 *          "sql": "SELECT some_identifier_col FROM my_table WHERE my_col IS NULL AND ETLFlowRunUID = [#flow_run_uid#] GROUP BY some_identifier_col"
 *      }],
 *      "result_action": {
 *          "alias": "exface.Core.CreateData",
 *          "object_alias": "my.App.object_to_save_messages,
 *          "input_mapper": {
 *              "from_object_alias": "exface.Core.DUMMY",
 *              "column_to_column_mappings": [
 *                  {"from": "[#message_text#]", "to":"Nachricht"},
 *                  {"from": "some_identifier_col", "to":"attribute_for_identifier"}
 *              ]
 *          }
 *      }
 *  }
 *  
 * ```
 * 
 * @author andrej.kabachnik
 *
 */
class SQLDataChecker extends AbstractETLPrototype
{    
    private $checks = [];
    
    private $sqlToGetCurrentIncrementValue = null;
    
    private $queries = [];
    
    private $resultActionUxon = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $flowRunUid, string $stepRunUid, ETLStepResultInterface $previousStepResult = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $connection = $this->getSqlConnection();
        $result = new IncrementalEtlStepResult($stepRunUid);
        $hitChecks = [];
        
        $phs = $this->getPlaceholders($flowRunUid, $stepRunUid, $lastResult);
        
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
        
        // Prepare all SQL queries
        foreach ($this->getChecks() as $i => $check) {
            $sql = $check->getSql();
            $sql = StringDataType::replacePlaceholders($sql, $phs, true, true);
        
            // Execute the main statement
            $query = new SqlDataQuery();
            $query->setSql($sql);
            $query->forceMultipleStatements(true);
            $this->queries[$i] = $query;
        }
        
        // Fire OnBeforeETLStepRun event once all SQL queries are calculated to make sure, the
        // createDebugWidget() method has access to the queries and the any logging/debugging
        // listeners can use it.
        $this->getWorkbench()->eventManager()->dispatch(new OnBeforeETLStepRun($this));
        
        // Perform SQL queries and store results. Perform all of them even if some produce hits
        // We want to see all check results at once!
        foreach ($this->getChecks() as $i => $check) {
            $query = $this->queries[$i];
            $query = $connection->query($query);
            $rows = $query->getResultArray($query);
            if (! empty($rows)) {
                $hitChecks[] = $check;
                $hitData[] = $rows;
            }
            $query->freeResult();
        }
        
        // If no hits, simply return the empty result
        if (empty($hitChecks)) {
            yield "SQL checks performed: " . count($this->getChecks()) . '. No hits.' . PHP_EOL;
            return $result;
        } 
        
        // If there are hits, render their messages (replace placeholders, etc.)
        // One message per row returned by each check
        yield "SQL checks hit:" . PHP_EOL;
        $messages = [];
        $rows = [];
        $stop = false;
        foreach ($hitChecks as $i => $check) {
            if ($check->getStopFlowOnHit()) {
                $stop = true;
            }
            $hitRows = $hitData[$i];
            foreach ($hitRows as $row) {
                $msgPhs = $phs;
                foreach ($row as $key => $val) {
                    $msgPhs[$key] = $val;
                }
                $msg = StringDataType::replacePlaceholders($check->getMessageText(), $msgPhs, true, true);
                $rows[] = array_merge($row, [DataColumn::sanitizeColumnName('[#message_text#]') => $msg]);
                yield "  - " . $msg . PHP_EOL;
                $messages[] = $msg;
            }
        }
        
        $result->setProcessedRowsCounter(count($messages));
        
        // Perform action on messages if required
        if (null !== $actionUxon = $this->getResultActionUxon()) {
            $actionPhs = array_merge($phs, ['message_text' => '[#message_text#]']);
            $actionUxon = UxonObject::fromJson(StringDataType::replacePlaceholders($actionUxon->toJson(), $actionPhs, false, false));
            $action = ActionFactory::createFromUxon($this->getWorkbench(), $actionUxon);
            $inputSheet = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'exface.Core.DUMMY');
            $inputSheet->addRows($rows);
            $task = TaskFactory::createFromDataSheet($inputSheet);
            $actionResult = $action->handle($task);
            yield '  ' . $actionResult->getMessage();
        }
        
        if ($stop === true) {
            throw new DataQueryFailedError($this->getCombinedQuery(), 'Data checks failed: ' . implode('; ', $messages));
        }
        
        return $result;
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
     * @return SqlDataCheck[]
     */
    protected function getChecks() : array
    {
        return $this->checks;
    }
    
    /**
     * SQL checks to be performed
     * 
     * @uxon-property checks
     * @uxon-type \axenox\ETL\Common\SqlDataCheck[]
     * @uxon-template [{"message_text": "", "sql": ""}]
     * @uxon-required true
     * 
     * @param UxonObject $array
     * @return SQLDataChecker
     */
    protected function setChecks(UxonObject $array) : SQLDataChecker
    {
        foreach ($array as $uxon) {
            $this->checks[] = new SqlDataCheck($uxon);
        }
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
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Common\AbstractETLPrototype::getPlaceholders()
     */
    protected function getPlaceholders(string $flowRunUid, string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        return array_merge(parent::getPlaceholders($flowRunUid, $stepRunUid, $lastResult),[
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
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isIncremental()
     */
    public function isIncremental() : bool
    {
        foreach ($this->getChecks() as $check) {
            if (stripos($check->getSql(), '[#last_run_') !== false) {
                return true;
            }
        }
        return $this->sqlToGetCurrentIncrementValue !== null;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        if (null !== $query = $this->getCombinedQuery()) {
            $debug_widget = $query->createDebugWidget($debug_widget);
        }
        return $debug_widget;
    }
    
    protected function getCombinedQuery() : ?SqlDataQuery
    {
        if (empty($this->queries)) {
            return null;
        }
        $sql = '';
        foreach ($this->queries as $q) {
            $sql .= $q->getSql() . PHP_EOL . PHP_EOL;
        }
        $combinedQuery = new SqlDataQuery();
        $combinedQuery->setSql($sql);
        return $combinedQuery;
    }
    
    protected function getResultActionUxon() : ?UxonObject
    {
        return $this->resultActionUxon;
    }
    
    /**
     * Action to handle (e.g. save) check messages if any checks hit
     * 
     * 
     * 
     * @uxon-property result_action
     * @uxon-type \exface\Core\CommonLogic\AbstractAction
     * @uxon-template {"alias": "exface.Core.CreateData", "object_alias": "", "input_mapper": {"column_to_column_mappings": [{"from": "[#message_text#]", "to": ""}, {"from": "", "to": ""}]}}
     * 
     * @param UxonObject $value
     * @return SQLDataChecker
     */
    protected function setResultAction(UxonObject $value) : SQLDataChecker
    {
        $this->resultActionUxon = $value;
        return $this;
    }
}