<?php
namespace axenox\ETL\Common;

class IncrementalEtlStepResult extends UxonEtlStepResult
{
    private $incrementValue = null;
    
    public function getIncrementValue() : ?string
    {
        return $this->incrementValue;
    }
    
    public function setIncrementValue($value) : IncrementalEtlStepResult
    {
        $this->incrementValue = $value;
        return $this;
    }
    
    public function exportUxonObject()
    {
        $uxon = parent::exportUxonObject();
        if ($this->incrementValue !== null) {
            $uxon->setProperty('increment_value', $this->getIncrementValue());
        }
        return $uxon;
    }
}