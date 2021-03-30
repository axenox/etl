<?php
namespace axenox\ETL\Events\Flow;

use exface\Core\Events\AbstractEvent;
use exface\Core\Interfaces\Contexts\ContextInterface;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\Interfaces\WidgetInterface;

/**
 * Event triggered when a context was initialized within a context scope.
 *
 * @event axenox.ETL.Flow.OnBeforeETLStepRun
 *
 * @author Andrej Kabachnik
 *
 */
class OnBeforeETLStepRun extends AbstractEvent
{
    private $step = null;
    
    private $debugWidget = null;
    
    public function __construct(ETLStepInterface $step)
    {
        $this->step = $step;
    }
    
    /**
     * {@inheritdoc}
     * @see \exface\Core\Events\AbstractEvent::getEventName()
     */
    public static function getEventName() : string
    {
        return 'axenox.ETL.Flow.OnBeforeETLStepRun';
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\WorkbenchDependantInterface::getWorkbench()
     */
    public function getWorkbench()
    {
        return $this->step->getWorkbench();
    }
    
    /**
     * 
     * @return ETLStepInterface
     */
    public function getStep(): ETLStepInterface
    {
        return $this->step;
    }
}