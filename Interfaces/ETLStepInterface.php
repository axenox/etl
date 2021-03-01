<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Interfaces\iCanBeConvertedToUxon;

interface ETLStepInterface extends WorkbenchDependantInterface, iCanBeConvertedToUxon
{
    public function getFromObject() : MetaObjectInterface;
    
    public function getToObject() : MetaObjectInterface;
    
    public function run(string $stepRunUid, string $previousStepRunUid = null) : \Generator;
    
    public function validate() : \Generator;
}