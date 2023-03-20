<?php
namespace axenox\ETL\Common;

use exface\Core\CommonLogic\AppInstallers\MetaModelAdditionInstaller;
use exface\Core\Interfaces\Selectors\SelectorInterface;
use exface\Core\Interfaces\InstallerContainerInterface;

/**
 * Makes sure data flows and their steps are exported with the apps metamodel
 * 
 * Add this to the app containing intermediate objects for your ETL processes (e.g. stages):
 * 
 * ```
 * $installer = new DataFlowInstaller($this->getSelector(), $container);
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
class DataFlowInstaller extends MetaModelAdditionInstaller
{
    /**
     * 
     * @param SelectorInterface $selectorToInstall
     * @param InstallerContainerInterface $installerContainer
     */
    public function __construct(SelectorInterface $selectorToInstall, InstallerContainerInterface $installerContainer)
    {
        parent::__construct($selectorToInstall, $installerContainer);
        $modelFolder = 'ETL';
        $this->addModelDataSheet($modelFolder, $this->createModelDataSheet('axenox.ETL.flow', 'app', 'MODIFIED_ON'));
        $this->addModelDataSheet($modelFolder, $this->createModelDataSheet('axenox.ETL.step', 'flow__app', 'MODIFIED_ON'));
    }
}