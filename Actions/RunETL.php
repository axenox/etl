<?php
namespace axenox\ETL\Actions;

use exface\Core\CommonLogic\AbstractActionDeferred;
use exface\Core\Interfaces\Tasks\TaskInterface;
use exface\Core\Interfaces\DataSources\DataTransactionInterface;
use exface\Core\Interfaces\Tasks\ResultMessageStreamInterface;
use exface\Core\Interfaces\Actions\iCanBeCalledFromCLI;
use exface\Core\CommonLogic\Actions\ServiceParameter;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\SortingDirectionsDataType;
use exface\Core\Exceptions\Actions\ActionRuntimeError;
use exface\Core\CommonLogic\UxonObject;
use axenox\ETL\Interfaces\ETLStepInterface;
use exface\Core\DataTypes\UUIDDataType;
use exface\Core\Factories\MetaObjectFactory;
use exface\Core\DataTypes\PhpFilePathDataType;

class RunETL extends AbstractActionDeferred implements iCanBeCalledFromCLI
{
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performImmediately()
     */
    protected function performImmediately(TaskInterface $task, DataTransactionInterface $transaction, ResultMessageStreamInterface $result) : array
    {
        return [$this->getJobAliases($task)];
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\CommonLogic\AbstractActionDeferred::performDeferred()
     */
    protected function performDeferred(array $jobs = []) : \Generator
    {
        foreach ($jobs as $alias) {
            yield from $this->runJob($alias);
        }
    }
        
    protected function runJob(string $alias) : \Generator
    {
        $jobRunUid = UUIDDataType::generateSqlOptimizedUuid();
        
        yield 'Running ETL job "' . $alias . '":';
        
        $prevStepRunUid = null;
        foreach ($this->getSteps($alias) as $step) {
            $stepRunUid = UUIDDataType::generateSqlOptimizedUuid();
            
            
            yield from $step->run($stepRunUid, $prevStepRunUid);
            $prevStepRunUid = $stepRunUid;
        }
    }
    
    /**
     * 
     * @param string $jobAlias
     * @throws ActionRuntimeError
     * @return ETLStepInterface[]
     */
    protected function getSteps(string $jobAlias) : array
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.step');
        $ds->getFilters()->addConditionFromString('job__alias', $jobAlias, ComparatorDataType::EQUALS);
        $ds->getSorters()->addFromString('job_pos', SortingDirectionsDataType::ASC);
        $ds->getColumns()->addMultiple([
            'job__name',
            'job__UID',
            'etl_prototype_path',
            'etl_config_uxon',
            'from_object',
            'to_object'
        ]);
        $ds->dataRead();
        
        if ($ds->isEmpty()) {
            throw new ActionRuntimeError($this, 'ETL job "' . $jobAlias . '" not found!');
        }
        
        $steps = [];
        $vendorPath = $this->getWorkbench()->filemanager()->getPathToVendorFolder();
        foreach ($ds->getRows() as $row) {
            $fromObj = MetaObjectFactory::createFromString($this->getWorkbench(), $row['from_object']);
            $toObj = MetaObjectFactory::createFromString($this->getWorkbench(), $row['to_object']);
            $prototypePath = $row['etl_prototype_path'];
            $class = PhpFilePathDataType::findClassInFile($vendorPath . DIRECTORY_SEPARATOR . $prototypePath);
            $steps[] = new $class($fromObj, $toObj, UxonObject::fromAnything($row['etl_config_uxon'] ?? []));
            break;
        }
        
        return $steps;
    }
    
    protected function getJobAliases(TaskInterface $task) : array
    {
        return ['TEST_TRACKPROD'];
    }
    
    public function getCliArguments(): array
    {
        return [
            (new ServiceParameter($this))
            ->setName('alias')
            ->setDescription('Namespaced alias of the ETL job to run.')
        ];
    }

    public function getCliOptions(): array
    {
        return [];
    }
}