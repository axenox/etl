<?php
namespace axenox\ETL\Common\Selectors;

use exface\Core\Interfaces\Selectors\VersionSelectorInterface;
use exface\Core\CommonLogic\Selectors\Traits\AliasSelectorTrait;
use exface\Core\CommonLogic\Selectors\AbstractSelector;
use exface\Core\CommonLogic\Selectors\Traits\UidSelectorTrait;
use exface\Core\CommonLogic\Selectors\Traits\VersionSelectorTrait;
use axenox\ETL\Interfaces\DataFlowSelectorInterface;

/**
 *
 * @author Andrej Kabachnik
 *
 */
class DataFlowSelector extends AbstractSelector implements DataFlowSelectorInterface
{
    use AliasSelectorTrait;
    
    use UidSelectorTrait;
    
    use VersionSelectorTrait;
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\Selectors\SelectorInterface::getComponentType()
     */
    public function getComponentType() : string
    {
        return 'data flow';
    }
}