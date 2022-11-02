<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\Interfaces\iCanGenerateDebugWidgets;

interface ETLStepInterface extends WorkbenchDependantInterface, iCanBeConvertedToUxon, iCanGenerateDebugWidgets
{
    public function getFromObject() : MetaObjectInterface;
    
    public function getToObject() : MetaObjectInterface;
    
    /**
     * 
     * @param string $flowRunUid
     * @param string $stepRunUid
     * @param ETLStepResultInterface $previousStepResult
     * @param ETLStepResultInterface $lastResult
     * @return \Generator|string[]|ETLStepResultInterface
     */
    public function run(string $flowRunUid, string $stepRunUid, ETLStepResultInterface $previousStepResult = null, ETLStepResultInterface $lastResult = null) : \Generator;
    
    public function validate() : \Generator;
    
    public function isDisabled() : bool;
    
    public function setDisabled(bool $value) : ETLStepInterface;
    
    public function getName() : string;
    
    /**
     * Returns the maximum time this step is allowed to run in seconds.
     * 
     * @return int
     */
    public function getTimeout() : int;
    
    /**
     *
     * @param string $resultData
     * @return ETLStepResultInterface
     */
    public static function parseResult(string $stepRunUid, string $resultData = null) : ETLStepResultInterface;
    
    public function isIncremental() : bool;
}