<?php
namespace axenox\ETL\Common;

use exface\Core\Interfaces\iCanBeConvertedToUxon;
use exface\Core\CommonLogic\Traits\ImportUxonObjectTrait;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\DataTypes\LogLevelDataType;

class SqlDataCheck implements iCanBeConvertedToUxon
{
    use ImportUxonObjectTrait;
    
    private $uxon = null;
    
    private $sql = null;
    
    private $messageText = null;
    
    private $stopFlowOnHit = true;
    
    private $logLevel;
    
    public function __construct(UxonObject $uxon)
    {
        $this->uxon = $uxon;
        $this->importUxonObject($uxon);
    }
    
    public function getSql() : string
    {
        return $this->sql;
    }
    
    /**
     * The SQL to run (supports multiple statements!)
     *
     * Available placeholders:
     *
     * - `[#from_object_address#]`
     * - `[#to_object_address#]`
     * - `[#flow_run_uid#]`
     * - `[#step_run_uid#]`
     * - `[#last_run_uid#]`
     * - `[#last_run_increment_value#]`
     *
     * If it is an incremental step, additional placeholders are available:
     *
     * - `[#current_increment_value#]`
     *
     * @uxon-property sql
     * @uxon-type string
     * @uxon-required true
     *
     * @param string $value
     * @return SqlDataCheck
     */
    protected function setSql(string $value) : SQLDataCheck
    {
        $this->sql = $value;
        return $this;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanBeConvertedToUxon::exportUxonObject()
     */
    public function exportUxonObject()
    {
        return $this->uxon;
    }
    
    /**
     * 
     * @return string
     */
    public function getMessageText() : string
    {
        return $this->messageText;
    }
    
    /**
     * The message to show if this check hits
     * @param string $value
     * @return SqlDataCheck
     */
    protected function setMessageText(string $value) : SqlDataCheck
    {
        $this->messageText = $value;
        return $this;
    }
    
    /**
     * 
     * @return bool
     */
    public function getStopFlowOnHit() : bool
    {
        return $this->stopFlowOnHit;
    }
    
    /**
     * Set to FALSE to register a check hit, but continue the flow run
     * 
     * @uxon-property stop_flow_on_hit
     * @uxon-type boolean
     * 
     * @param bool $value
     * @return SqlDataCheck
     */
    protected function setStopFlowOnHit(bool $value) : SqlDataCheck
    {
        $this->stopFlowOnHit = $value;
        return $this;
    }
    
    /**
     * 
     * @return string|NULL
     */
    public function getLogLevel() : ?string
    {
        return $this->logLevel;
    }
    
    /**
     * Specifiy a custom log level for a hit of this check.
     * 
     * If not set, the `log_level` of the checker flow step will be used.
     * 
     * @uxon-property log_level
     * @uxon-type [debug,info,notice,warning,error,critical,alert,emergency]
     * 
     * @param string $level
     * @return SqlDataCheck
     */
    protected function setLogLevel(string $level) : SqlDataCheck
    {
        $this->logLevel = LogLevelDataType::cast($level);
        return $this;
    }
}