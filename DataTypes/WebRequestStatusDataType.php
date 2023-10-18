<?php
namespace axenox\ETL\DataTypes;

use exface\Core\DataTypes\IntegerDataType;
use exface\Core\Interfaces\DataTypes\EnumDataTypeInterface;
use exface\Core\CommonLogic\DataTypes\EnumStaticDataTypeTrait;

/**
 * 
 * @author andrej.kabachnik
 *
 */
class WebRequestStatusDataType extends IntegerDataType implements EnumDataTypeInterface
{
    use EnumStaticDataTypeTrait;
    
    CONST RECEIVED = 10;
    CONST PROCESSING = 20;
    CONST ERROR = 70;
    CONST CANCELED = 90;
    CONST DONE = 99;
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\DataTypes\EnumDataTypeInterface::getLabels()
     */
    public function getLabels()
    {
        if (empty($this->labels)) {
            $translator = $this->getWorkbench()->getApp('axenox.ETL')->getTranslator();
            
            foreach (WebRequestStatusDataType::getValuesStatic() as $const => $val) {
                $this->labels[$val] = $translator->translate('WEBSERVICE.STATUS.' . $const);
            }
        }
        
        return $this->labels;
    }
    
}