<?php
namespace axenox\ETL\Common;

use axenox\ETL\ETLPrototypes\StepGroup;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
use exface\Core\Interfaces\WorkbenchInterface;
use axenox\ETL\Interfaces\DataFlowInterface;

/**
 * Represents an entire data flow
 * 
 * Technically, the flow has some metadata (like a name, a UID, etc.) and a root
 * StepGroup, which is executed whenever the flow is being run.
 * 
 * @author Andrej Kabachnik
 *
 */
class DataFlow implements DataFlowInterface
{
    use ImportUxonObjectTrait;
    
    private $alias = null;
    
    private $version = null;
    
    private $uid;
    
    private $rootStepGroup = null;
    
    public function __construct(WorkbenchInterface $workbench, string $uid, string $name, string $alias, string $version = null)
    {
        $this->workbench = $workbench;
        $this->uid = $uid;
        $this->name = $name;
        $this->alias = $alias;
        $this->version = $version;
    }
    
    /**
     * 
     * @return StepGroup
     */
    protected function getStepGroup() : StepGroup
    {
        if ($this->rootStepGroup === null) {
            $this->rootStepGroup = new StepGroup($this, $this->getName());
        }
        return $this->rootStepGroup;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::getAlias()
     */
    public function getAlias() : string
    {
        return $this->alias;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::getVersion()
     */
    public function getVersion() : ?string
    {
        return $this->version;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::getUid()
     */
    public function getUid() : string
    {
        return $this->uid;
    }
    
    /**
     * 
     * @return string
     */
    public function printExecutionPlan() : string
    {
        return '';
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::getName()
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::run()
     */
    public function run(ETLStepDataInterface $stepData): \Generator
    {
        yield from $this->getStepGroup()->run($stepData);
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\DataFlowInterface::getTimeout()
     */
    public function getTimeout(): int
    {
        return $this->getStepGroup()->getTimeout();
    }

    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\WorkbenchDependantInterface::getWorkbench()
     */
    public function getWorkbench()
    {
        return $this->workbench;
    }
}