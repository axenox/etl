<?php
namespace axenox\ETL\Common;

use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;

class UxonEtlStepResult implements ETLStepResultInterface
{
    use ImportUxonObjectTrait;
    
    private $uxon = null;
    
    private $stepRunUid = null;
    
    private $processedRowsCnt = null;
    
    public function __construct(string $stepRunUid, string $encodedData = null)
    {
        $this->uxon = UxonObject::fromAnything($encodedData);
        $this->importUxonObject($this->uxon);
        $this->stepRunUid = $stepRunUid;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepResultInterface::countProcessedRows()
     */
    public function countProcessedRows() : ?int
    {
        return $this->processedRowsCnt;
    }
    
    public function setProcessedRowsCounter(int $value) : ETLStepResultInterface
    {
        $this->processedRowsCnt = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepResultInterface::__toString()
     */
    public function __toString(): string
    {
        return $this->exportUxonObject()->toJson();
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject(bool $forceAllProperties = false)
    {
        $uxon = $this->uxon ?? new UxonObject();
        
        if ($forceAllProperties) {
            $uxon->setProperty('processed_rows_counter', $this->countProcessedRows());
        }
        
        return $uxon;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepResultInterface::getStepRunUid()
     */
    public function getStepRunUid(): string
    {
        return $this->stepRunUid;
    }
}