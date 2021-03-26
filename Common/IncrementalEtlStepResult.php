<?php
namespace axenox\ETL\Common;

class IncrementalEtlStepResult extends UxonEtlStepResult
{
    private $incrementValue = null;
    
    /**
     * 
     * @return string|NULL
     */
    public function getIncrementValue() : ?string
    {
        return $this->incrementValue;
    }
    
    /**
     * 
     * @param mixed $value
     * @return IncrementalEtlStepResult
     */
    public function setIncrementValue($value) : IncrementalEtlStepResult
    {
        $this->incrementValue = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Common\UxonEtlStepResult::exportUxonObject()
     */
    public function exportUxonObject(bool $forceAllProperties = false)
    {
        $uxon = parent::exportUxonObject();
        if ($forceAllProperties === true || $this->incrementValue !== null) {
            $uxon->setProperty('increment_value', $this->getIncrementValue() ?? '');
        }
        return $uxon;
    }
}