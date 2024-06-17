<?php
namespace axenox\ETL;

use exface\Core\Interfaces\InstallerInterface;
use exface\Core\CommonLogic\Model\App;
use exface\Core\CommonLogic\AppInstallers\AbstractSqlDatabaseInstaller;
use exface\Core\Facades\AbstractHttpFacade\HttpFacadeInstaller;
use axenox\ETL\Facades\DataFlowFacade;
use exface\Core\Factories\FacadeFactory;
use exface\Core\CommonLogic\AppInstallers\DataInstaller;

class ETLApp extends App
{
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\Model\App::getInstaller()
     */
    public function getInstaller(InstallerInterface $injected_installer = null)
    {
        $installer = parent::getInstaller($injected_installer);
        
        // Facade
        $tplInstaller = new HttpFacadeInstaller($this->getSelector());
        $tplInstaller->setFacade(FacadeFactory::createFromString(DataFlowFacade::class, $this->getWorkbench()));
        $installer->addInstaller($tplInstaller);
        
        // SQL schema
        $modelLoader = $this->getWorkbench()->model()->getModelLoader();
        $modelDataSource = $modelLoader->getDataConnection();
        $installerClass = get_class($modelLoader->getInstaller()->getInstallers()[0]);
        $schema_installer = new $installerClass($this->getSelector());
        if ($schema_installer instanceof AbstractSqlDatabaseInstaller) {
            $schema_installer
            ->setFoldersWithMigrations(['InitDB','Migrations'])
            ->setDataConnection($modelDataSource)
            ->setFoldersWithStaticSql(['Views'])
            ->setMigrationsTableName('_migrations_etl');
            
            $installer->addInstaller($schema_installer); 
        } else {
            $this->getWorkbench()->getLogger()->error('Cannot initialize DB installer for app "' . $this->getSelector()->toString() . '": the cores model loader installer must be compatible with AbstractSqlDatabaseInstaller!');
        }
        
        $dataInstaller = new DataInstaller($this->getSelector(), 'Data');
        $dataInstaller->addDataToReplace('axenox.ETL.webservice_type', 'CREATED_ON', 'app');
        $installer->addInstaller($dataInstaller);
        
        return $installer;
    }
}