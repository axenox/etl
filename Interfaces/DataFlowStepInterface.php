<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;
use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\Interfaces\iCanGenerateDebugWidgets;

/**
 * 
 * @author Andrej Kabachnik
 *
 */
interface DataFlowStepInterface extends WorkbenchDependantInterface, iCanBeConvertedToUxon, iCanGenerateDebugWidgets
{
    /**
     * 
     * @param ETLStepDataInterface $stepData
     * @return \Generator|string[]|ETLStepResultInterface
     */
    public function run(ETLStepDataInterface $stepData) : \Generator;
    
    public function isDisabled() : bool;
    
    public function setDisabled(bool $value) : DataFlowStepInterface;
    
    public function getName() : string;
    
    /**
     * Returns the maximum time this step is allowed to run in seconds.
     * 
     * @return int
     */
    public function getTimeout() : int;
    
    /**
     *
     * @return bool
     */
    public function isIncremental() : bool;
    
    /**
     *
     * @param string $resultData
     * @return ETLStepResultInterface
     */
    public static function parseResult(string $stepRunUid, string $resultData = null) : ETLStepResultInterface;
}