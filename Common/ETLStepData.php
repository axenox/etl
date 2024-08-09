<?php
namespace axenox\ETL\Common;

use axenox\ETL\Interfaces\ETLStepDataInterface;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\Interfaces\Tasks\TaskInterface;

class ETLStepData implements ETLStepDataInterface
{
	private TaskInterface $task;
	
	private string $flowRunUid;
	
	private ?string $stepRunUid;
	
	private ?ETLStepResultInterface $previousStepResult;
	
	private ?ETLStepResultInterface $lastResult;

    /**
     * 
     * @param TaskInterface $task
     * @param string $flowRunUid
     * @param string $stepRunUid
     * @param ETLStepResultInterface $previousStepResult
     * @param ETLStepResultInterface $lastResult
     * @param string $openApiJson
     */
    public function __construct(
		TaskInterface $task,
		string $flowRunUid,
		string $stepRunUid = null,
		ETLStepResultInterface $previousStepResult = null, 
		ETLStepResultInterface $lastResult = null
    ) 
    {
		$this->task = $task;
		$this->flowRunUid = $flowRunUid;
		$this->stepRunUid = $stepRunUid;
		$this->previousStepResult = $previousStepResult;
		$this->lastResult = $lastResult;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::getFlowRunUid()
	 */
	public function getFlowRunUid() : string 
	{
		return $this->flowRunUid;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::getStepRunUid()
	 */
	public function getStepRunUid() : string
	{		
		return $this->stepRunUid;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::setStepRunUid()
	 */
	public function setStepRunUid(string $value) : ETLStepDataInterface
	{
	    $this->stepRunUid = $value;
	    return $this;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::getPreviousResult()
	 */
	public function getPreviousResult( ): ?ETLStepResultInterface
	{
		return $this->previousStepResult;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::getLastResult()
	 */
	public function getLastResult() : ?ETLStepResultInterface
	{
		return $this->lastResult;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\ETLStepDataInterface::getTask()
	 */
	public function getTask() : TaskInterface
	{
		return $this->task;
	}
}