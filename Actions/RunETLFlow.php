<?php
namespace axenox\ETL\Actions;

use exface\Core\CommonLogic\AbstractActionDeferred;
use exface\Core\Interfaces\Tasks\TaskInterface;
use exface\Core\Interfaces\DataSources\DataTransactionInterface;
use exface\Core\Interfaces\Tasks\ResultMessageStreamInterface;
use exface\Core\Interfaces\Actions\iCanBeCalledFromCLI;
use exface\Core\CommonLogic\Actions\ServiceParameter;
use exface\Core\Exceptions\Actions\ActionRuntimeError;
use exface\Core\DataTypes\UUIDDataType;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\Exceptions\ActionExceptionInterface;
use exface\Core\Exceptions\Actions\ActionInputMissingError;
use exface\Core\Exceptions\Actions\ActionConfigurationError;
use exface\Core\Interfaces\Actions\iModifyData;
use exface\Core\Interfaces\Model\ExpressionInterface;
use exface\Core\Factories\ExpressionFactory;
use exface\Core\Exceptions\Actions\ActionInputError;
use axenox\ETL\Common\ETLStepData;
use axenox\ETL\Common\DataFlow;
use axenox\ETL\Factories\DataFlowFactory;

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
    	return [$task, $this->getFlows($task)];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performDeferred()
     */
    protected function performDeferred(TaskInterface $task = null, array $flows = []) : \Generator
    {
        foreach ($flows as $uid => $flow) {
            yield from $this->runFlow($flow, $uid, $task);
        }
    }
    
    /**
     * 
     * @param string $alias
     * @throws null
     * @return \Generator|string[]
     */
    protected function runFlow(DataFlow $flow, string $flowRunUid, TaskInterface $task) : \Generator
    {
        $indent = '  ';
        
        yield 'Running ETL flow "' . $flow->getName() . '" (run-UID ' . $flowRunUid . ').' . PHP_EOL;
        yield PHP_EOL . $indent . 'Execution plan:' . PHP_EOL;

        $timeout = $flow->getTimeout();
        if ($timeout > (ini_get('max_execution_time') ?? 30)) {
            yield PHP_EOL . 'Increasing PHP max execution time to ' . $timeout . ' seconds.';
            set_time_limit($timeout);
        }
        
        yield PHP_EOL . 'Starting now...' . PHP_EOL . PHP_EOL;
        
        try {
            $stepData = new ETLStepData($task, $flowRunUid, null, null, null);
            yield from $flow->run($stepData);
        } catch (\Throwable $e) {
            if (! $e instanceof ActionExceptionInterface) {
                $e = new ActionRuntimeError($this, $e->getMessage(), null, $e);
            }
            throw $e;
        }
        
        yield PHP_EOL . '✓ Finished successfully' . PHP_EOL;
    }
    
    /**
     * Returns an array of flows to run with flow run UIDs for keys and DataFlow instances as values.
     * 
     * @param TaskInterface $task
     * @throws ActionInputMissingError
     * @return DataFlow[]
     */
    protected function getFlows(TaskInterface $task) : array
    {
        $flows= [];
        $wb = $this->getWorkbench();
        switch (true) {
            case null !== $alias = $this->getFlowAlias():
                $flows = [DataFlowFactory::createFromString($wb, $alias)];
                break;
            case $task->hasParameter('flow'):
                foreach (explode(',', $task->getParameter('flow')) as $alias) {
                    $flows[] = DataFlowFactory::createFromString($wb, $alias);
                }
                break;
            case $task->hasInputData():
                $inputData = $this->getInputDataSheet($task);
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
                foreach ($col->getValues() as $alias) {
                    $flows[] = DataFlowFactory::createFromString($wb, $alias);
                }
                break;
        }
        
        if (empty($flows)) {
            throw new ActionInputMissingError($this, 'No ETL flow to run: please provide `flow` parameter or input data based on the flow object (axenox.ETL.flow)!');
        }
        
        $uidExpr = $this->getInputFlowRunUidExpression();
        switch (true) {
            case $task->hasInputData() && $uidExpr !== null:
                $inputData = $inputData ?? $this->getInputDataSheet($task);
                $flowsPerRunUid = [];
                if ($uidCol = $inputData->getColumns()->getByExpression($uidExpr)) {
                    foreach ($flows as $rowNo => $flow) {
                        $flowsPerRunUid[$uidCol->getValue($rowNo)] = $flow;
                    }
                } else {
                    foreach ($flows as $flow) {
                        $flowsPerRunUid[self::generateFlowRunUid()] = $flow;
                    }
                }
                break;
            case $task->hasParameter('run_uid'):
                $uids = explode(',', $task->getParameter('run_uid'));
                if (count($uids) !== count($flows)) {
                    throw new ActionInputError($this, 'The number of provided flow aliases (' . count($flows) . ') does not match the number number of run UIDs (' . count($uids) . ')!');
                }
                $flowsPerRunUid = array_combine($uids, $flows);
                break;
            case $uidExpr !== null && count($flows) === 1:
                $flowsPerRunUid = [$uidExpr->evaluate() => $flows[0]];
                break;
            default:
                $flowsPerRunUid = [];
                foreach ($flows as $selector) {
                    $flowsPerRunUid[self::generateFlowRunUid()] = $selector;
                }
                break;
        }
        
        return $flowsPerRunUid;
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