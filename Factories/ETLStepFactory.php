<?php
namespace axenox\ETL\Factories;

use exface\Core\Factories\AbstractStaticFactory;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\DataTypes\PhpFilePathDataType;
use exface\Core\CommonLogic\UxonObject;

abstract class ETLStepFactory extends AbstractStaticFactory
{
    public static function createFromFile(
        string $pathRelativeToVendorFolder, 
        string $name, 
        MetaObjectInterface $toObject,
        MetaObjectInterface $fromObject = null,
        UxonObject $uxon = null) : ETLStepInterface
    {
        $workbench = $toObject->getWorkbench();
        $vendorPath = $workbench->filemanager()->getPathToVendorFolder();
        $class = PhpFilePathDataType::findClassInFile($vendorPath . DIRECTORY_SEPARATOR . $pathRelativeToVendorFolder);
        return new $class($name, $toObject, $fromObject, $uxon);
    }
}