<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\WorkbenchDependantInterface;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\Interfaces\iCanGenerateDebugWidgets;
use exface\Core\Interfaces\Tasks\TaskInterface;

interface ETLStepDataInterface
{
	public function getFlowRunUid() : string;
	
	public function getStepRunUid() : string;
	
	public function getPreviousResult( ): ETLStepResultInterface;
	
	public function getLastResult() : ETLStepResultInterface;
	
	public function getTask() : TaskInterface;	
}