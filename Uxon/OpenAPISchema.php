<?php
namespace axenox\ETL\Uxon;

use exface\Core\CommonLogic\UxonObject;
use exface\Core\Interfaces\UxonSchemaInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Uxon\UxonSchema;
use exface\Core\Interfaces\WorkbenchInterface;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\ComparatorDataType;
use axenox\ETL\Facades\Helper\MetaModelSchemaBuilder;
use exface\Core\Factories\MetaObjectFactory;

/**
 * UXON-schema class for Composer auth.json.
 * 
 * @link https://getcomposer.org/doc/articles/authentication-for-private-packages.md
 * 
 * @author Andrej Kabachnik
 *
 */
class OpenAPISchema implements UxonSchemaInterface
{
    private $parentSchema = null;
    private $workbench = null;
    
    /**
     *
     * @param WorkbenchInterface $workbench
     */
    public function __construct(WorkbenchInterface $workbench, UxonSchema $parentSchema = null)
    {
        $this->parentSchema = $parentSchema;
        $this->workbench = $workbench;
    }
    
    /**
     *
     * @return string
     */
    public static function getSchemaName() : string
    {
        return 'composer.json Config';
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getValidValues()
     */
    public function getValidValues(UxonObject $uxon, array $path, string $search = null, string $rootPrototypeClass = null, MetaObjectInterface $rootObject = null): array
    {
        return [];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getParentSchema()
     */
    public function getParentSchema(): UxonSchemaInterface
    {
        return $this->parentSchema ?? new UxonSchema($this->getWorkbench(), $this);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPrototypeClass()
     */
    public function getPrototypeClass(UxonObject $uxon, array $path, string $rootPrototypeClass = null): string
    {
        return '\\' . __CLASS__;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPropertiesTemplates()
     */
    public function getPropertiesTemplates(string $prototypeClass): array
    {
        return [];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPropertyValueRecursive()
     */
    public function getPropertyValueRecursive(UxonObject $uxon, array $path, string $propertyName, string $rootValue = '')
    {
        return null;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getProperties()
     */
    public function getProperties(string $prototypeClass): array
    {
        return [];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::hasParentSchema()
     */
    public function hasParentSchema()
    {
        return $this->parentSchema !== null;
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
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPropertyTypes()
     */
    public function getPropertyTypes(string $prototypeClass, string $property): array
    {
        return [];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getMetaObject()
     */
    public function getMetaObject(UxonObject $uxon, array $path, MetaObjectInterface $rootObject = null): MetaObjectInterface
    {
        return $rootObject;
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPresets()
     */
    public function getPresets(UxonObject $uxon, array $path, string $rootPrototypeClass = null) : array
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'exface.Core.UXON_PRESET');
        $ds->getColumns()->addMultiple(['UID','NAME', 'PROTOTYPE__LABEL', 'DESCRIPTION', 'PROTOTYPE', 'UXON' , 'WRAP_PATH', 'WRAP_FLAG']);
        $ds->getFilters()->addConditionFromString('UXON_SCHEMA', '\\' . __CLASS__, ComparatorDataType::EQUALS);
        $ds->getSorters()
        ->addFromString('PROTOTYPE', SortingDirectionsDataType::ASC)
        ->addFromString('NAME', SortingDirectionsDataType::ASC);
        $ds->dataRead();
        
             
        $objectAlias = end($path);
        if (($path ?? '') !== ''){
            $objectSheet = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'exface.Core.OBJECT');
            $objectSheet->getFilters()->addConditionFromString('ALIAS_WITH_NS', $objectAlias, ComparatorDataType::IS);
            $objectSheet->getColumns()->addMultiple([
                'NAME',
                'ALIAS_WITH_NS'
            ]);
            $objectSheet->dataRead();
            $schemaBuilder = new MetaModelSchemaBuilder();
            foreach ($objectSheet->getRows() as $objRow) {
                $obj = MetaObjectFactory::createFromString($this->getWorkbench(), $objRow['ALIAS_WITH_NS']);
                $json = $schemaBuilder::transformIntoJsonSchema($obj, []);
            
                $ds->addRow([
                    'UID' => $obj->getId(),
                    'NAME' => $obj->getName(), 
                    'PROTOTYPE__LABEL' => 'Meta objects', 
                    'DESCRIPTION' => '', 
                    'PROTOTYPE' => 'object', 
                    'UXON' => json_encode($json), 
                    'WRAP_PATH' => null, 
                    'WRAP_FLAG' => 0
                ]);
            }
        }
        
        return $ds->getRows();
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getUxonType()
     */
    public function getUxonType(UxonObject $uxon, array $path, string $rootPrototypeClass = null) : ?string
    {
        return 'string';
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\UxonSchemaInterface::getPropertiesByAnnotation()
     */
    public function getPropertiesByAnnotation(string $annotation, $value, string $prototypeClass = null): array
    {
        return [];
    }
}