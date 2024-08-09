<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\Model\MetaObjectInterface;

interface ETLStepInterface extends DataFlowStepInterface
{
    /**
     * 
     * @return MetaObjectInterface
     */
    public function getFromObject() : MetaObjectInterface;
    
    /**
     * 
     * @return MetaObjectInterface
     */
    public function getToObject() : MetaObjectInterface;
}