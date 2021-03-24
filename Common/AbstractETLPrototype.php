<?php
namespace axenox\ETL\Common;

use exface\Core\Interfaces\WorkbenchInterface;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use axenox\ETL\Interfaces\ETLStepResultInterface;

abstract class AbstractETLPrototype implements ETLStepInterface
{
    use ImportUxonObjectTrait;
    
    private $workbench = null;
    
    private $uxon = null;
    
    private $stepRunUidAttributeAlias = null;
    
    private $fromObject = null;
    
    private $toObject = null;
    
    private $name = null;
    
    private $disabled = null;
    
    private $timeout = (60 * 5);
    
    public function __construct(string $name, MetaObjectInterface $toObject, MetaObjectInterface $fromObject = null, UxonObject $uxon = null)
    {
        $this->workbench = $fromObject->getWorkbench();
        $this->uxon = $uxon;
        $this->fromObject = $fromObject;
        $this->toObject = $toObject;
        $this->name = $name;
        if ($uxon !== null) {
            $this->importUxonObject($uxon);
        }
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\WorkbenchDependantInterface::getWorkbench()
     */
    public function getWorkbench() : WorkbenchInterface
    {
        return $this->workbench;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject()
    {
        return $this->uxon ?? new UxonObject();
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getName()
     */
    public function getName() : string
    {
        return $this->name;
    }
    
    /**
     * 
     * @param string $name
     * @return string|UxonObject
     */
    protected function getConfigProperty(string $name)
    {
        return $this->uxon->getProperty($name);
    }
    
    /**
     * 
     * @param string $name
     * @return bool
     */
    protected function hasConfigProperty(string $name) : bool
    {
        return $this->uxon->hasProperty($name);
    }
    
    protected function getStepRunUidAttributeAlias() : ?string
    {
        return $this->stepRunUidAttributeAlias;
    }
    
    /**
     * Alias of the attribute of the to-object where the UID of every step run is to be saved
     * 
     * @uxon-property step_run_uid_attribute_alias
     * @uxon-type metamodel:attribute
     * 
     * @param string $value
     * @return AbstractETLPrototype
     */
    protected function setStepRunUidAttributeAlias(string $value) : AbstractETLPrototype
    {
        $this->stepRunUidAttributeAlias = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getFromObject()
     */
    public function getFromObject() : MetaObjectInterface
    {
        return $this->fromObject;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getToObject()
     */
    public function getToObject() : MetaObjectInterface
    {
        return $this->toObject;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isDisabled()
     */
    public function isDisabled() : bool
    {
        return $this->disabled;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::setDisabled()
     */
    public function setDisabled(bool $value) : ETLStepInterface
    {
        $this->disabled = $value;
        return $this;
    }
    
    /**
     * 
     * @return string
     */
    public function __toString() : string
    {
        return $this->getName();
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::getTimeout()
     */
    public function getTimeout() : int
    {
        return $this->timeout;
    }
    
    /**
     * Number of seconds the step is allowed to run at maximum.
     * 
     * @uxon-property timeout
     * @uxon-type integer
     * 
     * @param int $seconds
     * @return ETLStepInterface
     */
    public function setTimeout(int $seconds) : ETLStepInterface
    {
        $this->timeout = $seconds;
        return $this;
    }
    
    /**
     * Returns an array with names and values for placeholders that can be used in the steps config.
     * 
     * @param string $stepRunUid
     * @param ETLStepResultInterface $lastResult
     * @return array
     */
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        $phs = [
            'step_run_uid' => $stepRunUid
        ];
        
        if ($lastResult !== null) {
            $phs['last_run_uid'] = $lastResult->getStepRunUid();
            foreach ($lastResult->exportUxonObject(true)->toArray() as $ph => $val) {
                if (is_scalar($val)) {
                    $phs['last_run_' . $ph] = $val;
                }
            }
        }
        
        return $phs;
    }
}