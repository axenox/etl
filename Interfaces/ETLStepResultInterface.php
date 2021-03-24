<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\CommonLogic\UxonObject;

interface ETLStepResultInterface extends iCanBeConvertedToUxon
{
    public function __construct(string $sterRunUid, string $encodedData = null);
    
    /**
     * 
     * @return string
     */
    public function getStepRunUid() : string;
    
    /**
     * 
     * @return int|NULL
     */
    public function countProcessedRows() : ?int;
    
    /**
     * 
     * @return string
     */
    public function __toString() : string;
    
    /**
     * Returns the UXON representation of the result.
     * 
     * If `$forceAllProperties` is `false` (default), the UXON will only contain properties
     * actually used. If `true`, all supported properties must be included (possibly with
     * empty values).
     * 
     * @param bool $forceAllProperties
     * @return UxonObject
     * 
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject(bool $forceAllProperties = false);
}