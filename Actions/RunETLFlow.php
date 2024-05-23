<?php
namespace axenox\ETL\Actions;

use exface\Core\CommonLogic\AbstractActionDeferred;
use exface\Core\Interfaces\Tasks\TaskInterface;
use exface\Core\Interfaces\DataSources\DataTransactionInterface;
use exface\Core\Interfaces\Tasks\ResultMessageStreamInterface;
use exface\Core\Interfaces\Actions\iCanBeCalledFromCLI;
use exface\Core\CommonLogic\Actions\ServiceParameter;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\Exceptions\Actions\ActionRuntimeError;
use exface\Core\CommonLogic\UxonObject;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\DataTypes\UUIDDataType;
use exface\Core\Factories\MetaObjectFactory;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Factories\ETLStepFactory;
use exface\Core\DataTypes\DateTimeDataType;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\Exceptions\ExceptionInterface;
use exface\Core\Factories\UiPageFactory;
use exface\Core\Interfaces\Exceptions\ActionExceptionInterface;
use exface\Core\Exceptions\Actions\ActionInputMissingError;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\Exceptions\Actions\ActionConfigurationError;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use exface\Core\Factories\WidgetFactory;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\Interfaces\Actions\iModifyData;
use exface\Core\DataTypes\ArrayDataType;
use exface\Core\Interfaces\Model\MetaAttributeInterface;
use exface\Core\Interfaces\Model\ExpressionInterface;
use exface\Core\Factories\ExpressionFactory;
use exface\Core\Exceptions\Actions\ActionInputError;
use axenox\ETL\Common\ETLStepData;

/**
 * Runs one or multiple ETL flows
 * 
 * ## Examples for buttons
 * 
 * ### Run a single flow
 * 
 * ```
 *  {
 *      "alias": "axenox.ETL.RunETLFlow",
 *      "flow_alias": "..."
 *  }
 * 
 * ```
 * 
 * ### Run a flow defined in the input data
 * 
 * ```
 *  {
 *      "alias": "axenox.ETL.RunETLFlow",
 *      "input_flow_alias": "<attribute_alias>"
 *  }
 * 
 * ```
 * 
 * ### Run a flow with a custom generated UID
 * 
 * ```
 *  {
 *    "alias": "exface.Core.ActionChain",
 *    "actions": [
 *      {
 *        "alias": "exface.Core.UpdateData",
 *        "input_mapper": {
 *          "inherit_columns": "own_system_attributes",
 *          "column_to_column_mappings": [
 *            {
 *              "from": "=UniqueId()",
 *              "to": "flow_run"
 *            }
 *          ]
 *        }
 *      },
 *      {
 *        "alias": "axenox.etl.RunETLFlow",
 *        "flow_alias": "...",
 *        "input_flow_run_uid": "flow_run"
 *      }
 *    ]
 *  }
 *  
 * ```
 * 
 * ## CLI examples
 * 
 * ```
 *  vendor/bin/action axenox.ETL:RunETLFLow my_flow_alias
 *  
 * ```
 * 
 * @author andrej.kabachnik
 *
 */
class RunETLFlow extends AbstractActionDeferred implements iCanBeCalledFromCLI, iModifyData
{
    private $stepsLoaded = [];
    
    private $stepsPerFlowUid = [];
    
    private $flowStoppers = [];
    
    private $flowToRun = null;
    
    private $flowRunUid = null;
    
    private $inputFlowAlias = null;
    
    private $inputFlowRunUidExpr = null;
    private ?string $openApiJson = null;

    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performImmediately()
     */
    protected function performImmediately(TaskInterface $task, DataTransactionInterface $transaction, ResultMessageStreamInterface $result) : array
    {
    	return [$task, $this->getFlowAliases($task)];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performDeferred()
     */
    protected function performDeferred(TaskInterface $task = null, array $flows = []) : \Generator
    {
        foreach ($flows as $uid => $alias) {
            yield from $this->runFlow($alias, $uid, $task);
        }
    }
    
    /**
     * 
     * @param string $alias
     * @throws null
     * @return \Generator|string[]
     */
    protected function runFlow(string $alias, string $flowRunUid, TaskInterface $task) : \Generator
    {
        $indent = '  ';
        
        yield 'Running ETL flow "' . $alias . '" (run-UID ' . $flowRunUid . ').' . PHP_EOL;
        yield PHP_EOL . $indent . 'Execution plan:' . PHP_EOL;
        
        $prevStepResult = null;

        $steps = $this->getSteps($alias, $indent.$indent);
        $timeout = 0;
        foreach ($steps as $step) {
            $timeout += $step->getTimeout();
        }

        if ($timeout > (ini_get('max_execution_time') ?? 30)) {
            yield PHP_EOL . 'Increasing PHP max execution time to ' . $timeout . ' seconds.';
            set_time_limit($timeout);
        }
        
        yield PHP_EOL . 'Starting now...' . PHP_EOL . PHP_EOL;
        foreach ($steps as $pos => $step) {
            $nr = $pos+1;
            $prevRunResult = $this->getPreviousResultData($step);
            $logRow = $this->logRunStart($step, $flowRunUid, $nr, $prevRunResult)->getRow(0);
            
            $stepRunUid = $logRow['UID'];
            yield $indent . $nr . '. ' . $step->getName() . ': ';
            if ($step->isDisabled()) {
                yield 'disabled' . PHP_EOL;
            } else {
                $log = '';
                $stepData = new ETLStepData($task, $flowRunUid, $stepRunUid, $prevStepResult, $prevRunResult, $this->openApiJson);
                $this->getWorkbench()->eventManager()->addListener(OnBeforeETLStepRun::getEventName(), function(OnBeforeETLStepRun $event) use (&$logRow, $step) {
                    if ($event->getStep() !== $step) {
                        return;
                    }
                    $ds = $this->logRunDebug($event, $logRow);
                    $logRow = $ds->getRow(0);
                });
                try {
                	$generator = $step->run($stepData);
                    foreach ($generator as $msg) {
                        $msg = $indent . $indent . $msg;
                        $log .= $msg;
                        yield $msg;
                    }
                    $stepResult = $generator->getReturn();
                    $this->logRunSuccess($logRow, $log, $stepResult);
                } catch (\Throwable $e) {
                    if (! $e instanceof ActionExceptionInterface) {
                        $e = new ActionRuntimeError($this, $e->getMessage(), null, $e);
                    }
                    try {
                        $this->logRunError($logRow, $e, $log);
                    } catch (\Throwable $el) {
                        $this->getWorkbench()->getLogger()->logException($el);
                        yield PHP_EOL . $indent 
                        .  '✗ Could not save ETL run log: ' . $el->getMessage() 
                        . ' in ' . $el->getFile() 
                        . ' on line ' . $el->getLine();
                    }
                    if ($this->getStopFlowOnError($step)) {
                        throw $e;
                    } else {
                        yield PHP_EOL . '✗ ERROR: ' . $e->getMessage();
                        yield PHP_EOL . '  See log-ID ' . $e->getId() . ' for details!';
                        $this->getWorkbench()->getLogger()->logException($e);
                    }
                }
            }
            
            $prevStepResult = $stepResult;
        }
        
        yield PHP_EOL . '✓ Finished successfully' . PHP_EOL;
    }
    
    /**
     * 
     * @param OnBeforeETLStepRun $event
     * @param array $row
     * @return \exface\Core\Interfaces\DataSheets\DataSheetInterface
     */
    protected function logRunDebug(OnBeforeETLStepRun $event, array $row)
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        try {
            $debugContainer = WidgetFactory::createDebugMessage($this->getWorkbench(), $ds->getMetaObject());
            $widgetJson = $event->getStep()->createDebugWidget($debugContainer)->exportUxonObject()->toJson();
            $row['debug_widget'] = $widgetJson;
            $ds->addRow($row);
            $ds->dataUpdate();
        } catch (\Throwable $e) {
            // Forget the widget if rendering does not work
            $this->getWorkbench()->getLogger()->logException($e);
            $ds->addRow($row);
        }
        return $ds;
    }
    
    /**
     * 
     * @param array $row
     * @param string $output
     * @param ETLStepResultInterface $result
     * @return DataSheetInterface
     */
    protected function logRunSuccess(array $row, string $output, ETLStepResultInterface $result = null) : DataSheetInterface
    {
        $time = DateTimeDataType::now();
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        $row['end_time'] = $time;
        $row['result_count'] = $result->countProcessedRows() ?? 0;
        $row['result_uxon'] = $result->__toString();
        $row['success_flag'] = true;
        $row['output'] = $output;
        
        if (($result instanceof IncrementalEtlStepResult) && $result->getIncrementValue() !== null) {
            $row['incremental_flag'] = true;
        } else {
            $row['incremental_flag'] = false;
            $row['incremental_after_run'] = null;
        }
        
        $ds->addRow($row);
        $ds->dataUpdate();
        return $ds;
    }
    
    /**
     * 
     * @param array $row
     * @param ExceptionInterface $exception
     * @param string $output
     * @return DataSheetInterface
     */
    protected function logRunError(array $row, ExceptionInterface $exception, string $output = '') : DataSheetInterface
    {
        $time = DateTimeDataType::now();
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        $row['end_time'] = $time;
        $row['output'] = $output;
        $row['error_flag'] = true;
        $row['error_message'] = $exception->getMessage();
        $row['error_log_id'] = $exception->getId();
        try {
            $widgetJson = $exception->createWidget(UiPageFactory::createEmpty($this->getWorkbench()))->exportUxonObject()->toJson();
            $row['error_widget'] = $widgetJson;
        } catch (\Throwable $e) {
            // Forget the widget if rendering does not work
            $this->getWorkbench()->getLogger()->logException($e);
        }
        $ds->addRow($row);
        $ds->dataUpdate();
        return $ds;
    }
    
    /**
     * 
     * @param ETLStepInterface $step
     * @param string $flowRunUid
     * @param int $position
     * @param ETLStepResultInterface $lastResult
     * @return DataSheetInterface
     */
    protected function logRunStart(ETLStepInterface $step, string $flowRunUid, int $position, ETLStepResultInterface $lastResult = null) : DataSheetInterface
    {
        $time = DateTimeDataType::now();
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        $row = [
            'step' => $this->getStepUid($step),
            'flow' => $this->getFlowUid($step),
            'flow_run' => $flowRunUid,
            'flow_run_pos' => $position,
            'start_time' => $time,
            'timeout_seconds' => $step->getTimeout(),
            'incremental_flag' => $step->isIncremental(),
            'incremental_after_run' => $lastResult === null ? null : $lastResult->getStepRunUid()
        ];
        try {
            $debugContainer = WidgetFactory::createDebugMessage($this->getWorkbench(), $ds->getMetaObject());
            $widgetJson = $step->createDebugWidget($debugContainer)->exportUxonObject()->toJson();
            $row['error_widget'] = $widgetJson;
        } catch (\Throwable $e) {
            // Forget the widget if rendering does not work
            $this->getWorkbench()->getLogger()->logException($e);
        }
        if ($step->isDisabled()) {
            $row['step_disabled_flag'] = true;
            $row['end_time'] = $time;
        }
        $ds->getColumns()->addFromSystemAttributes();
        $ds->addRow($row);
        $ds->dataCreate();
        return $ds;
    }
    
    /**
     * 
     * @param ETLStepInterface $step
     * @throws ActionRuntimeError
     * @return string
     */
    protected function getStepUid(ETLStepInterface $step) : string
    {
        $uid = array_search($step, $this->stepsLoaded, true);
        if (! $uid) {
            throw new ActionRuntimeError($this, 'No UID found for ETL step "' . $step->__toString() . '": step not loaded/planned properly?');
        }
        return $uid;
    }
    
    /**
     * 
     * @param ETLStepInterface $step
     * @throws ActionRuntimeError
     * @return string
     */
    protected function getFlowUid(ETLStepInterface $step) : string
    {
        foreach ($this->stepsPerFlowUid as $uid => $steps) {
            if (! in_array($step, $steps, true)) {
                $uid = null;
            }
        }
        if (! $uid) {
            throw new ActionRuntimeError($this, 'No flow found for ETL step "' . $step->__toString() . '": step not loaded/planned properly?');
        }
        return $uid;
    }
    
    /**
     * 
     * @param string $flowAlias
     * @throws ActionRuntimeError
     * @return ETLStepInterface[]
     */
    protected function getSteps(string $flowAlias, string $logIndent) : array
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
        $ds->getFilters()->addConditionFromString('flow__alias', $flowAlias, ComparatorDataType::EQUALS);
        $ds->getSorters()->addFromString('step_flow_sequence', SortingDirectionsDataType::ASC);
        $ds->getColumns()->addMultiple([
            'UID',
            'name',
            'etl_prototype',
            'etl_config_uxon',
            'from_object',
            'to_object',
            'disabled',
            'flow',
            'stop_flow_on_error',
            'step_flow_sequence'
        ]);
        $ds->dataRead();
        
        if ($ds->isEmpty()) {
            return [];
        }
        
        $disabledCompletely = true;
        $steps = [];
        $loadedSteps = [];
        foreach ($ds->getRows() as $row) {
            $toObj = MetaObjectFactory::createFromString($this->getWorkbench(), $row['to_object']);
            if ($row['from_object']) {
                $fromObj = MetaObjectFactory::createFromString($this->getWorkbench(), $row['from_object']);
            } else {
                $fromObj = $toObj;
            }
            
            $step = ETLStepFactory::createFromFile(
                $row['etl_prototype'], 
                $row['name'], 
                $toObj,
                $fromObj,
                UxonObject::fromAnything($row['etl_config_uxon'] ?? [])
            );

            $step->setDisabled(BooleanDataType::cast($row['disabled']));
            if ($step->isDisabled() === false) {
                $disabledCompletely = false;
            }

            $steps[] = $step;
            $flowUId = $row['flow'];
            $loadedSteps[$row['UID']] = $step;

            if ($row['stop_flow_on_error']) {
                $this->flowStoppers[] = $step;
            }
        }
        
        if ($disabledCompletely === true) {
            return [];
        }

        $this->stepsLoaded = $loadedSteps;
        $this->stepsPerFlowUid[$flowUId] = $loadedSteps;
        return $steps;
    }
    
    /**
     * Returns an array of flows to run with flow run UIDs for keys and aliases for values.
     * 
     * @param TaskInterface $task
     * @throws ActionInputMissingError
     * @return array
     */
    protected function getFlowAliases(TaskInterface $task) : array
    {
        switch (true) {
            case $this->getFlowAlias():
                $aliases = [$this->getFlowAlias()];
                break;
            case $task->hasParameter('flow'):
                $aliases = explode(',', $task->getParameter('flow'));
                break;
            case $task->hasInputData():
                $inputData = $this->getInputDataSheet($task);
                $aliases = [];
                $aliasExpr = $this->getInputFlowAliasExpression($inputData);
                if (! $col = $inputData->getColumns()->getByExpression($aliasExpr)) {
                    if (! $inputData->hasUidColumn(true)) {
                        throw new ActionInputMissingError($this, 'Cannot determine data flow from input data!');
                    }
                    // TODO #DataPattern use DataPattern to load missing data here
                    $flowSheet = $inputData->copy()->extractSystemColumns();
                    $col = $flowSheet->getColumns()->addFromExpression($aliasExpr);
                    $flowSheet->getFilters()->addConditionFromColumnValues($flowSheet->getUidColumn());
                    $flowSheet->dataRead();
                }
                $aliases = $col->getValues();
                break;
        }
        
        if (empty($aliases)) {
            throw new ActionInputMissingError($this, 'No ETL flow to run: please provide `flow` parameter or input data based on the flow object (axenox.ETL.flow)!');
        }
        
        $uidExpr = $this->getInputFlowRunUidExpression();
        switch (true) {
            case $task->hasInputData() && $uidExpr !== null:
                $inputData = $inputData ?? $this->getInputDataSheet($task);
                $aliasesWithUids = [];
                if ($uidCol = $inputData->getColumns()->getByExpression($uidExpr)) {
                    foreach ($aliases as $rowNo => $alias) {
                        $aliasesWithUids[$uidCol->getValue($rowNo)] = $alias;
                    }
                } else {
                    foreach ($aliases as $alias) {
                        $aliasesWithUids[self::generateFlowRunUid()] = $alias;
                    }
                }
                break;
            case $task->hasParameter('run_uid'):
                $uids = explode(',', $task->getParameter('run_uid'));
                if (count($uids) !== count($aliases)) {
                    throw new ActionInputError($this, 'The number of provided flow aliases (' . count($aliases) . ') does not match the number number of run UIDs (' . count($uids) . ')!');
                }
                $aliasesWithUids = array_combine($uids, $aliases);
                break;
            case $uidExpr !== null && count($aliases) === 1:
                $aliasesWithUids = [$uidExpr->evaluate() => $aliases[0]];
                break;
            default:
                $aliasesWithUids = [];
                foreach ($aliases as $alias) {
                    $aliasesWithUids[self::generateFlowRunUid()] = $alias;
                }
                break;
        }
        
        return $aliasesWithUids;
    }
    
    /**
     * 
     * @param ETLStepInterface $step
     * @return string|NULL
     */
    protected function getPreviousResultData(ETLStepInterface $step) : ?ETLStepResultInterface
    {
        $sheet = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        $sheet->getFilters()->addConditionFromString('step', $this->getStepUid($step), ComparatorDataType::EQUALS);
        $sheet->getFilters()->addConditionFromString('success_flag', 1, ComparatorDataType::EQUALS);
        $sheet->getFilters()->addConditionFromString('invalidated_flag', 0, ComparatorDataType::EQUALS);
        $sheet->getSorters()->addFromString('start_time', SortingDirectionsDataType::DESC);
        $sheet->getColumns()->addMultiple([
            'UID',
            'result_uxon'
        ]);
        $sheet->dataRead(1);
        if (! $sheet->isEmpty()) {
            return $step::parseResult($sheet->getCellValue('UID', 0), $sheet->getCellValue('result_uxon', 0));
        } else {
            return null;
        }
    }
    
    /**
     * 
     * @param ETLStepInterface $step
     * @return bool
     */
    protected function getStopFlowOnError(ETLStepInterface $step) : bool
    {
        return in_array($step, $this->flowStoppers, true);
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\Actions\iCanBeCalledFromCLI::getCliArguments()
     */
    public function getCliArguments(): array
    {
        return [
            (new ServiceParameter($this))
            ->setName('flow')
            ->setDescription('Namespaced alias of the ETL flow to run or comma-separated list to run multiple flows')
            ->setRequired(true)
        ];
    }

    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\Actions\iCanBeCalledFromCLI::getCliOptions()
     */
    public function getCliOptions(): array
    {
        return [
            (new ServiceParameter($this))
            ->setName('run_uid')
            ->setDescription('UID for the flow run if not to be generated automatically. Use comma-separated list in case of multiple flow aliases.')
        ];
    }
    
    /**
     * 
     * @return string|NULL
     */
    protected function getFlowAlias() : ?string
    {
        return $this->flowToRun;
    }
    
    /**
     * Alias of the flow to run.
     * 
     * If not set, the flow alias will be determined from the task provided:
     * - task parameter `flow` or
     * - values from the `alias` column in the input data sheet if that contains flow data
     * 
     * @uxon-property flow_alias
     * @uxon-type metamodel:axenox.ETL.flow:alias
     * 
     * @param string $value
     * @return RunETLFlow
     */
    public function setFlowAlias(string $value) : RunETLFlow
    {
        $this->flowToRun = $value;
        return $this;
    }
    
    /**
     * 
     * @return string
     */
    public static function generateFlowRunUid() : string
    {
        return UUIDDataType::generateSqlOptimizedUuid();
    }
    
    /**
     * 
     * @return ExpressionInterface
     */
    protected function getInputFlowAliasExpression(DataSheetInterface $inputData) : ?ExpressionInterface
    {
        if ($this->inputFlowAlias === null) {
            if ($inputData->getMetaObject()->is('axenox.ETL.flow')) {
                return ExpressionFactory::createForObject($inputData->getMetaObject(), 'alias');
            } elseif ($inputData->getMetaObject()->is('axenox.ETL.webservice_request')) {
                return ExpressionFactory::createForObject($inputData->getMetaObject(), 'route__flow__alias');
            } else {
                throw new ActionConfigurationError($this, 'Cannot determine data flow to run!');
            }
        }
        return ExpressionFactory::createForObject($inputData->getMetaObject(), $this->inputFlowAlias);
    }
    
    /**
     * Column of the input data containing the aliases of the flows to run
     * 
     * @uxon-property input_flow_alias
     * @uxon-type metamodel:expression
     * 
     * @param string $value
     * @return RunETLFlow
     */
    public function setInputFlowAlias(string $value) : RunETLFlow
    {
        $this->inputFlowAlias = $value;
        return $this;
    }
    
    /**
     * 
     * @return ExpressionInterface
     */
    protected function getInputFlowRunUidExpression() : ?ExpressionInterface
    {
        return $this->inputFlowRunUidExpr;
    }
    
    /**
     * Column of the input data containing the UIDs for the flow runs (e.g. to reference the runs from outside)
     * 
     * @uxon-property input_flow_run_uid
     * @uxon-type metamodel:expression
     * 
     * @param string $value
     * @return RunETLFlow
     */
    public function setInputFlowRunUid(string $value) : RunETLFlow
    {
        $this->inputFlowRunUidExpr = ExpressionFactory::createForObject($this->getMetaObject(), $value);
        return $this;
    }

    public function setOpenApiJson(string $openApiJson) : void
    {
        $this->openApiJson = $openApiJson;
    }
}