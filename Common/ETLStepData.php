<?php
namespace axenox\ETL\Common;

use axenox\ETL\Interfaces\ETLStepDataInterface;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\Interfaces\Tasks\TaskInterface;

class ETLStepData implements ETLStepDataInterface
{
	private TaskInterface $task;
	
	private string $flowRunUid;
	
	private string $stepRunUid;
	
	private ?ETLStepResultInterface $previousStepResult;
	
	private ?ETLStepResultInterface $lastResult;
	
	public function __construct(
		TaskInterface $task,
		string $flowRunUid, 
		string $stepRunUid, 
		ETLStepResultInterface $previousStepResult = null, 
		ETLStepResultInterface $lastResult = null)
	{
		$this->task = $task;
		$this->flowRunUid = $flowRunUid;
		$this->stepRunUid = $stepRunUid;
		$this->previousStepResult = $previousStepResult;
		$this->lastResult = $lastResult;
	}
	
	public function getFlowRunUid() : string 
	{
		return $this->flowRunUid;
	}
	
	public function getStepRunUid() : string
	{		
		return $this->stepRunUid;
	}
	
	public function getPreviousResult( ): ETLStepResultInterface
	{
		return $this->previousStepResult;
	}
	
	public function getLastResult() : ETLStepResultInterface
	{
		return $this->lastResult;
	}
	
	public function getTask() : TaskInterface
	{
		return $this->task;
	}
}