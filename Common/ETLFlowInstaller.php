<?php
namespace axenox\ETL\Common;

use exface\Core\CommonLogic\AppInstallers\MetaModelAdditionInstaller;
use exface\Core\Interfaces\Selectors\SelectorInterface;
use exface\Core\Interfaces\InstallerContainerInterface;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\Exceptions\AppNotFoundError;

/**
 * Makes sure ETL flows and their steps are exported with the apps metamodel
 * 
 * Add this to the app containing intermediate objects for your ETL processes (e.g. stages):
 * 
 * ```
 * $installer = new ETLFlowInstaller($this->getSelector(), $container);
 * $container->addInstaller($installer);
 * 
 * ```
 * 
 * Technically, this installer will automatically find the MetaModelInstaller in your apps
 * installer container and add ETL objects using model additions.
 * 
 * @author Andrej Kabachnik
 *
 */
class ETLFlowInstaller extends MetaModelAdditionInstaller
{
    /**
     * 
     * @param SelectorInterface $selectorToInstall
     * @param InstallerContainerInterface $installerContainer
     */
    public function __construct(SelectorInterface $selectorToInstall, InstallerContainerInterface $installerContainer)
    {
        parent::__construct($selectorToInstall, $installerContainer);
        
        // If the app is not installed yet, we don't know it's UID,
        // but we also don't need it for this case - so just let it
        // be NULL.
        try {
            $appUid = $this->getApp()->getUid();
        } catch (AppNotFoundError $e) {
            $appUid = null;
        }
        
        $flowSheet = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.flow');
        $flowSheet->getFilters()->addConditionFromString('app', $appUid, ComparatorDataType::EQUALS);
        $flowSheet->getSorters()->addFromString('MODIFIED_ON', SortingDirectionsDataType::ASC);
        $flowSheet->getColumns()->addFromAttributeGroup($flowSheet->getMetaObject()->getAttributeGroup('~WRITABLE'));
        $this->addModelDataSheet('ETL', $flowSheet);
        
        $flowSheet = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
        $flowSheet->getFilters()->addConditionFromString('flow__app', $appUid, ComparatorDataType::EQUALS);
        $flowSheet->getSorters()->addFromString('MODIFIED_ON', SortingDirectionsDataType::ASC);
        $flowSheet->getColumns()->addFromAttributeGroup($flowSheet->getMetaObject()->getAttributeGroup('~WRITABLE'));
        $this->addModelDataSheet('ETL', $flowSheet);
    }
}