<?php
namespace axenox\ETL\Common;

use exface\Core\Interfaces\Selectors\SelectorInterface;
use exface\Core\Interfaces\InstallerContainerInterface;
use exface\Core\CommonLogic\AppInstallers\DataInstaller;
use exface\Core\CommonLogic\AppInstallers\MetaModelInstaller;

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
class DataFlowInstaller extends DataInstaller
{
    /**
     * 
     * @param SelectorInterface $selectorToInstall
     * @param InstallerContainerInterface $installerContainer
     */
    public function __construct(SelectorInterface $selectorToInstall)
    {
        parent::__construct($selectorToInstall, MetaModelInstaller::FOLDER_NAME_MODEL . DIRECTORY_SEPARATOR . 'ETL');
        
        $this->addDataToReplace('axenox.ETL.flow', 'CREATED_ON', 'app', [], 'Flows/[#alias#]/[#version#]/01_flow.json');
        $this->addDataToReplace('axenox.ETL.step', 'CREATED_ON', 'flow__app', [], 'Flows/[#flow__alias#]/[#flow__version#]/02_steps.json');
        $this->addDataToReplace('axenox.ETL.webservice_type', 'CREATED_ON', 'app');
        $this->addDataToReplace('axenox.ETL.webservice', 'CREATED_ON', 'app', [], 'WebServices/[#alias#]/[#version#]/04_webservice.json');
        $this->addDataToReplace('axenox.ETL.webservice_flow', 'CREATED_ON', 'webservice__app', [], 'WebServices/[#webservice__alias#]/[#webservice__version#]/05_webservice_flows.json');
    }
}