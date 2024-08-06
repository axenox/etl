<?php
namespace axenox\ETL\Factories;

use exface\Core\Factories\AbstractSelectableComponentFactory;
use exface\Core\Interfaces\Selectors\SelectorInterface;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\SemanticVersionDataType;
use axenox\ETL\Common\DataFlow;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Interfaces\WorkbenchInterface;
use axenox\ETL\Interfaces\DataFlowSelectorInterface;
use axenox\ETL\Common\Selectors\DataFlowSelector;
use exface\Core\Exceptions\UnexpectedValueException;

abstract class DataFlowFactory extends AbstractSelectableComponentFactory
{
    /**
     * 
     * @param DataFlowSelectorInterface $selector
     * @param array $constructorArguments
     * @return DataFlow
     */
    public static function createFromSelector(SelectorInterface $selector, array $constructorArguments = null) : DataFlow
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($selector->getWorkbench(), 'axenox.ETL.flow');
        $ds->getColumns()->addFromSystemAttributes();
        $ds->getColumns()->addMultiple([
            'name',
            'alias',
            'version'
        ]);
        if ($selector->isUid()) {
            $ds->getFilters()->addConditionFromAttribute($ds->getMetaObject()->getUidAttribute(), $selector->__toString(), ComparatorDataType::EQUALS);           
            $ds->dataRead();
            $row = $ds->getRow(0);
        } else {
            $ds->getFilters()->addConditionFromString('alias', $selector->stripVersion(), ComparatorDataType::EQUALS);
            $ds->dataRead();
            if ($ds->countRows() === 1) {
                $row = $ds->getRow(0);
            } else {
                $versionCol = $ds->getColumns()->get('version');
                $versions = $versionCol->getValues();
                $bestMatch = SemanticVersionDataType::findVersionBest($selector->getVersion(), $versions);
                if ($bestMatch === null) {
                    throw new UnexpectedValueException('Version "' . $selector->getVersion() . '" not found for data flow "' . $selector->stripVersion() . '"!');
                }
                $row = $ds->getRow($versionCol->findRowByValue($bestMatch));
            }
        }
        
        return new DataFlow($selector->getWorkbench(), $row['UID'], $row['name'], $row['alias'], 'version');
    }
    
    /**
     * 
     * @param WorkbenchInterface $workbench
     * @param string $aliasWithVersionOrUid
     * @return DataFlow
     */
    public static function createFromString(WorkbenchInterface $workbench, string $aliasWithVersionOrUid) : DataFlow
    {
        $selector = new DataFlowSelector($workbench, $aliasWithVersionOrUid);
        return static::createFromSelector($selector);
    }
}