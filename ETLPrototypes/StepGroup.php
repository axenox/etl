<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\Exceptions\InternalError;
use exface\Core\Exceptions\RuntimeException;
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
use axenox\ETL\Interfaces\DataFlowStepInterface;
use exface\Core\Factories\WidgetFactory;
use exface\Core\Exceptions\Actions\ActionRuntimeError;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\Factories\MetaObjectFactory;
use axenox\ETL\Factories\ETLStepFactory;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
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
    
    private $stepsLoaded = null;
    
    private $flowStoppers = [];
    
    private $flow = null;

    private $parentGroup = null;

    private $log = '';
    
    public function __construct(DataFlow $flow, string $name, StepGroup $parentGroup = null, UxonObject $uxon = null)
    {
        $this->flow = $flow;
        $this->parentGroup = $parentGroup;
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
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::run()
     */
    public function run(ETLStepDataInterface $stepData, int $startPosNo = null) : \Generator
    {
        $indent = '  ';
        $prevStepResult = null;
        $flowRunUid = $stepData->getFlowRunUid();
        $result = new UxonEtlStepResult(UUIDDataType::generateSqlOptimizedUuid());
        
        // IDEA the StepGroup itself does not produce any step run entry, just its children.
        // However, if the child is a step group too, there will be a step run entry for it
        // because the parent group will produce one. So maybe it is not such a bad idea to
        // have a step run for every step group. That would allow to save the log of the
        // entire flow and maybe also some input data.

        $steps = $this->getSteps();
        $nr = $startPosNo;
        foreach ($steps as $step) {
            $nr++;
            $prevRunResult = $this->getPreviousResultData($step);
            $logRow = $this->logRunStart($step, $flowRunUid, $nr, $prevRunResult)->getRow(0);
            $stepRunUid = $logRow['UID'];
            if ($step->isDisabled()) {
                yield $indent . $nr . '. ' . $step->getName() . ' - disabled' . PHP_EOL;
            } else {
                yield $indent . $nr . '. ' . $step->getName() . ':' . PHP_EOL;
                $log = '';
                $stepData = new ETLStepData($stepData->getTask(), $flowRunUid, $stepRunUid, $prevStepResult, $prevRunResult);
                
                $this->getWorkbench()->eventManager()->addListener(OnBeforeETLStepRun::getEventName(), function(OnBeforeETLStepRun $event) use (&$logRow, $step) {
                    if ($event->getStep() !== $step) {
                        return;
                    }
                    $ds = $this->logRunDebug($event, $logRow);
                    $logRow = $ds->getRow(0);
                });
                
                try {
                    $generator = $step->run($stepData, $nr);
                    foreach ($generator as $msg) {
                        $msg = $indent . $indent . $msg;
                        $log .= $msg;
                        yield $msg;
                    }
                    $this->log .= $log;
                    // TODO handling StepGroup explicitly differently here is not very elegant - 
                    // it would be better to use StepData and StepResult to transport the global
                    // sequence.
                    if ($step instanceof StepGroup) {
                        $nr += $step->countSteps();
                        $log = 'Ran ' . $step->countSteps() . ' steps';
                    }
                    $stepResult = $generator->getReturn();
                    $this->logRunSuccess($step, $logRow, $log, $stepResult);
                } catch (\Throwable $e) {
                    if ($step instanceof StepGroup) {
                        $nr += $step->countSteps();
                        $log = 'ERROR: one of the steps failed.';
                    }
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
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::getName()
     */
    public function getName() : string
    {
        return $this->name;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::isDisabled()
     */
    public function isDisabled() : bool
    {
        return $this->disabled;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::setDisabled()
     */
    public function setDisabled(bool $value) : DataFlowStepInterface
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
     * @see \axenox\ETL\Interfaces\DataFlowStepInterface::getTimeout()
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
    protected function logRunSuccess(DataFlowStepInterface $step, array $row, string $output, ETLStepResultInterface $result = null) : DataSheetInterface
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

        $debugContainer = WidgetFactory::createDebugMessage($this->getWorkbench(), $ds->getMetaObject());
        $widgetJson = $step->createDebugWidget($debugContainer)->exportUxonObject()->toJson();
        $row['debug_widget'] = $widgetJson;
        
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
     * @param DataFlowStepInterface $step
     * @param string $flowRunUid
     * @param int $position
     * @param ETLStepResultInterface $lastResult
     * @return DataSheetInterface
     */
    protected function logRunStart(DataFlowStepInterface $step, string $flowRunUid, int $position, ETLStepResultInterface $lastResult = null) : DataSheetInterface
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
     * @param DataFlowStepInterface $step
     * @throws ActionRuntimeError
     * @return string
     */
    protected function getStepUid(DataFlowStepInterface $step) : string
    {
        $uid = array_search($step, $this->getSteps(), true);
        if (! $uid) {
            throw new RuntimeException('No UID found for ETL step "' . $step->__toString() . '": step not loaded/planned properly?');
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
        if ($this->stepsLoaded !== null) {
            return $this->stepsLoaded;
        }
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
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
        if ($this->hasParent()) {
            $ds->getFilters()->addConditionFromString('parent_step', $this->getParentGroup()->getStepUid($this), ComparatorDataType::EQUALS);
        } else {
            $ds->getFilters()
                ->addConditionFromString('flow', $this->getFlow()->getUid(), ComparatorDataType::EQUALS)
                ->addConditionForAttributeIsNull('parent_step');
        }
        $ds->dataRead();
        
        if ($ds->isEmpty()) {
            return [];
        }
        
        $disabledCompletely = true;
        $steps = [];
        $loadedSteps = [];
        foreach ($ds->getRows() as $row) {
            $stepConfig = UxonObject::fromAnything($row['etl_config_uxon'] ?? []);
            if ($row['etl_prototype'] === 'axenox/etl/ETLPrototypes/StepGroup.php') {
                $step = new StepGroup($this->getFlow(), $row['name'], $this, $stepConfig);
            } else {
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
                    $stepConfig
                );
            }
            
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

    protected function countSteps() : int
    {
        return count($this->getSteps());
    }
    
    /**
     *
     * @param DataFlowStepInterface $step
     * @return bool
     */
    protected function getStopFlowOnError(DataFlowStepInterface $step) : bool
    {
        return in_array($step, $this->flowStoppers, true);
    }
    
    /**
     *
     * @param DataFlowStepInterface $step
     * @return string|NULL
     */
    protected function getPreviousResultData(DataFlowStepInterface $step) : ?ETLStepResultInterface
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

    /**
     * 
     * @return bool
     */
    protected function hasParent() : bool
    {
        return $this->parentGroup !== null;
    }

    /**
     * 
     * @return StepGroup
     */
    protected function getParentGroup() : ?StepGroup
    {
        return $this->parentGroup;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debugWidget)
    {
        if ($this->log !== null) {
            $tab = $debugWidget->createTab();
            $debugWidget->addTab($tab);
            $tab->setCaption($this->getName());
            $tab->setColumnsInGrid(1);
            $tab->addWidget(WidgetFactory::createFromUxonInParent($tab, new UxonObject([
                'widget_type' => 'Html',
                'html' => '<pre>' . ($this->log === '' ? 'No output' : $this->log) . '</pre>',
                'width' => 'max'
            ])));
        }
        return $debugWidget;
    }
}