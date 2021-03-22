<?php
namespace axenox\ETL\Common;

class IncrementalEtlStepResult extends UxonEtlStepResult
{
    private $incrementValue = null;
    
    public function getIncrementValue() : ?string
    {
        return $this->incrementValue;
    }
    
    public function setIncrementValue(string $value) : IncrementalEtlStepResult
    {
        $this->incrementValue = $value;
        return $this;
    }
}