<?php
namespace axenox\ETL\Common;

use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use axenox\ETL\Uxon\SqlColumnMappingSchema;

class SqlColumnMapping implements iCanBeConvertedToUxon
{
    use ImportUxonObjectTrait;
    
    private $fromObject = null;
    
    private $toObject = null;
    
    private $fromExpression = null;
    
    private $toExpression = null;
    
    public function __construct(MetaObjectInterface $fromObject, MetaObjectInterface $toObject, UxonObject $uxon)
    {
        $this->fromObject = $fromObject;
        $this->toObject = $toObject;
        $this->importUxonObject($uxon);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject()
    {
        $uxon = new UxonObject();
        // TODO
        return $uxon;
    }
    
    /**
     * 
     * @return string
     */
    protected function getFromExpression() : string
    {
        return $this->fromExpression;
    }
    
    public function getFromSql() : string
    {
        $expr = $this->getFromExpression();
        $obj = $this->getFromObject();
        switch (true) {
            case stripos($expr, '(') !== false:
                $sql = $expr;
                break;
            case $obj->hasAttribute($expr):
                $attr = $obj->getAttribute($expr);
                // TODO support data address properties?
                $sql = $attr->getDataAddress();
                break;
            default:
                $sql = $expr;
        }
        return $sql;
    }
    
    /**
     * Any use of this expression in the data sheet will be transformed to the to-expression in the mapped sheet.
     *
     * The expression can be an attribute alias or an SQL statement enclosed in parenthes `()`.
     *
     * @uxon-property from
     * @uxon-type metamodel:attribute|string
     *
     * @param string $string
     * @return SqlColumnMapping
     */
    protected function setFrom(string $string) : SqlColumnMapping
    {
        $this->fromExpression = $string;
        return $this;
    }
    
    /**
     * 
     * @return string
     */
    protected function getToExpression() : string
    {
        return $this->toExpression;
    }
    
    public function getToSql() : string
    {
        $expr = $this->getToExpression();
        $obj = $this->getToObject();
        switch (true) {
            case $obj->hasAttribute($expr): 
                $attr = $obj->getAttribute($expr);
                // TODO support data address properties?
                $sql = $attr->getDataAddress();
                break;
            default:
                $sql = $expr;
        }
        return $sql;
    }
    
    /**
     * This is the expression, that the from-expression is going to be translated to: alias of an attribute or an SQL column name.
     *
     * @uxon-property to
     * @uxon-type metamodel:attribute|string
     *
     * @param string $string
     * @return SqlColumnMapping
     */
    protected function setTo(string $string) : SqlColumnMapping
    {
        $this->toExpression = $string;
        return $this;
    }
    
    protected function getToObject() : MetaObjectInterface
    {
        return $this->toObject;
    }
    
    protected function getFromObject() : MetaObjectInterface
    {
        return $this->fromObject;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\WorkbenchDependantInterface::getWorkbench()
     */
    protected function getWorkbench()
    {
        return $this->getToObject()->getWorkbench();
    }
    
    /**
     *
     * {@inheritdoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::getUxonSchemaClass()
     */
    public static function getUxonSchemaClass() : ?string
    {
        return SqlColumnMappingSchema::class;
    }
}