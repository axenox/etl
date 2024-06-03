<?php
namespace axenox\ETL\Common;

use exface\Core\CommonLogic\AppInstallers\MetaModelAdditionInstaller;
use exface\Core\Interfaces\Selectors\SelectorInterface;
use exface\Core\Interfaces\InstallerContainerInterface;
use exface\Core\CommonLogic\AppInstallers\AbstractAppInstaller;

/**
 * @deprecated use DataFlowInstaller instead!
 * 
 * Makes sure data flows and their steps are exported with the apps metamodel
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
class ETLFlowInstaller extends AbstractAppInstaller
{
    private $additionInstaller = null;
    
    /**
     *
     * @param SelectorInterface $selectorToInstall
     * @param InstallerContainerInterface $installerContainer
     */
    public function __construct(SelectorInterface $selectorToInstall, InstallerContainerInterface $installerContainer)
    {
        $this->additionInstaller = (new MetaModelAdditionInstaller($selectorToInstall, $installerContainer, 'ETL'))
        ->addDataToReplace('axenox.ETL.flow', 'CREATED_ON', 'app')
        ->addDataToReplace('axenox.ETL.step', 'CREATED_ON', 'flow__app')
        ->addDataToReplace('axenox.ETL.webservice_type', 'CREATED_ON', 'app')
        ->addDataToReplace('axenox.ETL.webservice', 'CREATED_ON', 'app')
        ->addDataToReplace('axenox.ETL.webservice_flow', 'CREATED_ON', 'webservice__app');
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\InstallerInterface::backup()
     */
    public function backup(string $absolute_path): \Iterator
    {
        return $this->additionInstaller->backup($absolute_path);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\InstallerInterface::uninstall()
     */
    public function uninstall(): \Iterator
    {
        return $this->additionInstaller->uninstall();
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\InstallerInterface::install()
     */
    public function install(string $source_absolute_path): \Iterator
    {
        return $this->additionInstaller->install($source_absolute_path);
    }
}