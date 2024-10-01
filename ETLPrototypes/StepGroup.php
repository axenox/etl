<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\InternalError;
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\DataTypes\DateTimeDataType;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\Interfaces\Exceptions\ExceptionInterface;
use exface\Core\Factories\UiPageFactory;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\Factories\WidgetFactory;
use exface\Core\Exceptions\Actions\ActionRuntimeError;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\Factories\MetaObjectFactory;
use axenox\ETL\Factories\ETLStepFactory;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
use axenox\ETL\Interfaces\DataFlowStepInterface;
use axenox\ETL\Common\ETLStepData;
use exface\Core\Interfaces\WorkbenchInterface;
use axenox\ETL\Common\UxonEtlStepResult;
use axenox\ETL\Common\DataFlow;
use exface\Core\DataTypes\UUIDDataType;

/**
 * Runs multiple steps without any additional logic.
 * 
 * If a group is configured to continue on failure, any error inside the group will
 * skip subsequent steps within it, but the flow will continue with the next step
 * outside of the group. 
 * 
 * @author Andrej Kabachnik
 *
 */
class StepGroup implements DataFlowStepInterface
{
    use ImportUxonObjectTrait;
    
    private $uxon = null;
    
    private $workbench = null;
    
    private $name = null;
    
    private $stepsLoaded = [];
    
    private $flowStoppers = [];
    
    private $flow = null;
    
    public function __construct(DataFlow $flow, string $name, UxonObject $uxon = null)
    {
        $this->flow = $flow;
        $this->workbench = $flow->getWorkbench();
        $this->name = $name;
        $this->uxon = $uxon;
        if ($uxon !== null) {
            $this->importUxonObject($uxon);
        }
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        return $debug_widget;
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::run()
     */
    public function run(ETLStepDataInterface $stepData): \Generator
    {
        $indent = '  ';
        $prevStepResult = null;
        $flowRunUid = $stepData->getFlowRunUid();
        $result = new UxonEtlStepResult(UUIDDataType::generateSqlOptimizedUuid());
        
        $steps = $this->getSteps();
        
        yield PHP_EOL . $this->getName() . PHP_EOL . PHP_EOL;
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
                $stepData = new ETLStepData($stepData->getTask(), $flowRunUid, $stepRunUid, $prevStepResult, $prevRunResult, $this->openApiJson);
                
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
                    if (! $e instanceof ExceptionInterface) {
                        $e = new InternalError($e->getMessage(), null, $e);
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
        
        return $result;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::isIncremental()
     */
    public function isIncremental(): bool
    {
        foreach ($this->getSteps() as $step) {
            if (! $step->isIncremental()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        return new UxonEtlStepResult($stepRunUid, $resultData);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\WorkbenchDependantInterface::getWorkbench()
     */
    public function getWorkbench() : WorkbenchInterface
    {
        return $this->workbench;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject()
    {
        return $this->uxon ?? new UxonObject();
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getName()
     */
    public function getName() : string
    {
        return $this->name;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isDisabled()
     */
    public function isDisabled() : bool
    {
        return $this->disabled;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::setDisabled()
     */
    public function setDisabled(bool $value) : ETLStepInterface
    {
        $this->disabled = $value;
        return $this;
    }
    
    /**
     *
     * @return string
     */
    public function __toString() : string
    {
        return $this->getName();
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getTimeout()
     */
    public function getTimeout() : int
    {
        $val = 0;
        foreach ($this->getSteps() as $step) {
            $val += $step->getTimeout();
        }
        return $val;
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
            'flow' => $this->getFlow()->getUid(),
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
     * @param string $flowAlias
     * @throws ActionRuntimeError
     * @return DataFlowStepInterface[]
     */
    protected function getSteps() : array
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
        $ds->getFilters()->addConditionFromString('flow', $this->getFlow()->getUid(), ComparatorDataType::EQUALS);
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
            $loadedSteps[$row['UID']] = $step;
            
            if ($row['stop_flow_on_error']) {
                $this->flowStoppers[] = $step;
            }
        }
        
        if ($disabledCompletely === true) {
            return [];
        }
        
        $this->stepsLoaded = $loadedSteps;
        return $steps;
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
    
    public function getFlow() : DataFlow
    {
        return $this->flow;
    }
}