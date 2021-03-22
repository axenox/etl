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

class RunETLFlow extends AbstractActionDeferred implements iCanBeCalledFromCLI
{
    private $stepsLoaded = [];
    
    private $stepsPerFlowUid = [];
    
    private $stepsPreviousRsults = [];
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performImmediately()
     */
    protected function performImmediately(TaskInterface $task, DataTransactionInterface $transaction, ResultMessageStreamInterface $result) : array
    {
        return [$this->getFlowAliases($task)];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performDeferred()
     */
    protected function performDeferred(array $flows = []) : \Generator
    {
        foreach ($flows as $alias) {
            yield from $this->runFlow($alias);
        }
    }
    
    /**
     * 
     * @param string $alias
     * @throws null
     * @return \Generator|string[]
     */
    protected function runFlow(string $alias) : \Generator
    {
        $flowRunUid = UUIDDataType::generateSqlOptimizedUuid();
        
        yield 'Running ETL flow "' . $alias . '" (run-UID ' . $flowRunUid . ')' . PHP_EOL;
        
        $prevStepRunUid = null;
        $indent = '  ';
        foreach ($this->getSteps($alias) as $pos => $step) {
            $prevRunResult = $this->getPreviousResultData($step);
            $logRow = $this->logRunStart($step, $flowRunUid, $pos, $prevRunResult)->getRow(0);
            $stepRunUid = $logRow['UID'];
            yield $indent . $step->getName() . ': ';
            if ($step->isDisabled()) {
                yield 'disabled' . PHP_EOL;
            } else {
                $log = '';
                try {
                    $generator = $step->run($stepRunUid, $prevStepRunUid);
                    foreach ($generator as $msg) {
                        $msg = $indent . $indent . $msg;
                        $log .= $msg;
                        yield $msg;
                    }
                    $this->logRunSuccess($logRow, $log, $generator->getReturn());
                } catch (\Throwable $e) {
                    if (! $e instanceof ActionExceptionInterface) {
                        $e = new ActionRuntimeError($this, $e->getMessage(), null, $e);
                    }
                    try {
                        $this->logRunError($logRow, $e, $log);
                    } catch (\Throwable $el) {
                        $this->getWorkbench()->getLogger()->logException($el);
                        yield PHP_EOL . 'Could not save ETL run log: ' . $el->getMessage() . ' in ' . $el->getFile() . ' on line ' . $el->getLine();
                    }
                    throw $e;
                }
            }
            
            $prevStepRunUid = $stepRunUid;
        }
        
        yield PHP_EOL . '✓ Finished successfully' . PHP_EOL;
    }
    
    protected function logRunSuccess(array $row, string $output, ETLStepResultInterface $result = null) : DataSheetInterface
    {
        $time = DateTimeDataType::now();
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step_run');
        $row['end_time'] = $time;
        $row['result_count'] = $result->countProcessedRows();
        $row['result_uxon'] = $result->__toString();
        $row['success_flag'] = true;
        $row['output'] = $output;
        $ds->addRow($row);
        $ds->dataUpdate();
        return $ds;
    }
    
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
    
    protected function logRunStart(ETLStepInterface $step, string $flowRunUid, int $position, ETLStepResultInterface $previousRunResult) : DataSheetInterface
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
            'result_uxon_of_prev_run' => $previousRunResult->__toString()
        ];
        if ($step->isDisabled()) {
            $row['step_disabled_flag'] = true;
            $row['end_time'] = $time;
        }
        $ds->getColumns()->addFromSystemAttributes();
        $ds->addRow($row);
        $ds->dataCreate();
        return $ds;
    }
    
    protected function getStepUid(ETLStepInterface $step) : string
    {
        $uid = array_search($step, $this->stepsLoaded);
        if (! $uid) {
            throw new ActionRuntimeError($this, 'No UID found for ETL step "' . $step->__toString() . '": step not loaded/planned properly?');
        }
        return $uid;
    }
    
    protected function getFlowUid(ETLStepInterface $step) : string
    {
        foreach ($this->stepsPerFlowUid as $uid => $steps) {
            if (! in_array($step, $steps)) {
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
    protected function getSteps(string $flowAlias) : array
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
        $ds->getFilters()->addConditionFromString('flow__alias', $flowAlias, ComparatorDataType::EQUALS);
        $ds->getSorters()->addFromString('level', SortingDirectionsDataType::ASC);
        $ds->getColumns()->addMultiple([
            'required_for_step',
            'UID',
            'name',
            'etl_prototype',
            'etl_config_uxon',
            'from_object',
            'to_object',
            'disabled',
            'flow'
        ]);
        $ds->dataRead();
        
        if ($ds->isEmpty()) {
            throw new ActionRuntimeError($this, 'ETL flow "' . $flowAlias . '" not found!');
        }
        
        /* Steps in original reading order with UIDs as keys. Planned steps are moved
         * from this array to $stepsPlanned.
         * @var $stepsToPlan \axenox\ETL\Interfaces\ETLStepInterface[] */
        $stepsToPlan = [];
        /* Steps in execution order with regular numeric keys starting with 0.
         * @var $stepsPlanned \axenox\ETL\Interfaces\ETLStepInterface[] */
        $stepsPlanned = [];
        /* Array with object aliases as keys and subsets of $stepsToPlan still required 
         * to populate each object as values. Once a step is planned it should be removed
         * from each step-array here it was contained in.
         * @var $stepsForObject \axenox\ETL\Interfaces\ETLStepInterface[][] */
        $stepsForObject = [];
        
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
            $stepsToPlan[$row['UID']] = $step;
            $stepsForObject[$toObj->getAliasWithNamespace()][$row['UID']] = $step;
            $flowUId = $row['flow'];
        }
        
        $this->stepsLoaded = $stepsToPlan;
        $this->stepsPerFlowUid[$flowUId] = $stepsToPlan;
        
        $stepsCnt = count($stepsToPlan);
        for ($i = 0; $i < $stepsCnt; $i++) {
            foreach ($stepsToPlan as $stepUid => $step) {
                $stepsRequired = $step->getFromObject() ? ($stepsForObject[$step->getFromObject()->getAliasWithNamespace()] ?? []) : [];
                if (! in_array($step, $stepsPlanned) && (empty($stepsRequired) || (count($stepsRequired) === 1 && $stepsRequired[$stepUid] === $step))) {
                    $stepsPlanned[] = $step;
                    foreach ($stepsForObject as $o => $reqs) {
                        if (array_key_exists($stepUid, $reqs)) {
                            unset($stepsForObject[$o][$stepUid]);
                        }
                    }
                    unset($stepsToPlan[$stepUid]);
                    break;
                }
            }
        }
        
        if (count($stepsPlanned) !== $stepsCnt) {
            throw new RuntimeException('Cannot determine execution order for ETL steps!');
        }
        
        return $stepsPlanned;
    }
    
    protected function getFlowAliases(TaskInterface $task) : array
    {
        if ($task->hasParameter('flow')) {
            return explode(',', $task->getParameter('flow'));
        } else {
            $inputData = $this->getInputDataSheet($task);
            if ($inputData->getMetaObject()->is('axenox.ETL.flow') && $col = $inputData->getColumns()->get('host')) {
                return $col->getValues();
            }
        }
        throw new ActionInputMissingError($this, 'No ETL flow to run: please provide `flow` parameter or input data based on the flow object (axenox.ETL.flow)!');
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
        $sheet->getFilters()->addConditionFromString('success_flag', true, ComparatorDataType::EQUALS);
        $sheet->getSorters()->addFromString('start_time', SortingDirectionsDataType::DESC);
        $sheet->getColumns()->addMultiple([
            'UID',
            'result_uxon'
        ]);
        $sheet->dataRead(1);
        return $step::parseResult($sheet->getCellValue('UID', 0), $sheet->getCellValue('result_uxon', 0));
    }
    
    public function getCliArguments(): array
    {
        return [
            (new ServiceParameter($this))
            ->setName('flow')
            ->setDescription('Namespaced alias of the ETL flow to run.')
        ];
    }

    public function getCliOptions(): array
    {
        return [];
    }
}