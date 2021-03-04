<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Interfaces\iCanBeConvertedToUxon;

interface ETLStepInterface extends WorkbenchDependantInterface, iCanBeConvertedToUxon
{
    public function getFromObject() : MetaObjectInterface;
    
    public function getToObject() : MetaObjectInterface;
    
    public function run(string $stepRunUid, string $previousStepRunUid = null, string $incrementValue = null) : \Generator;
    
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
}