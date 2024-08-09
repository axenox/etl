<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;

interface DataFlowInterface extends WorkbenchDependantInterface
{
    public function getAlias() : string;
    
    public function getUid() : string;
    
    public function getVersion() : ?string;
    
    /**
     * 
     * @return string
     */
    public function printExecutionPlan() : string;
    
    public function getName(): string;

    public function run(ETLStepDataInterface $stepData): \Generator;

    public function getTimeout(): int;
}