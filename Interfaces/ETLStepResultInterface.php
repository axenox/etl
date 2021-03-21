<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\iCanBeConvertedToUxon;

interface ETLStepResultInterface extends iCanBeConvertedToUxon
{
    public function __construct(string $sterRunUid, string $encodedData = null);
    
    public function getStepRunUid() : string;
    
    public function countProcessedRows() : ?int;
    
    public function __toString() : string;
}