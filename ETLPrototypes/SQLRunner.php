<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractETLPrototype;
use exface\Core\Interfaces\DataSources\SqlDataConnectorInterface;
use exface\Core\DataTypes\StringDataType;

class SQLRunner extends AbstractETLPrototype
{
    private $sql = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, string $previousStepRunUid = null, string $incrementValue = null) : \Generator
    {
        $sql = $this->getSql();
        $sql = $this->replacePlaceholders($sql, $stepRunUid, $previousStepRunUid, $incrementValue);
        $connection = $this->getSqlConnection();
        $query = $connection->runSql($sql, true);
        // TODO get some results from the query here?
        $query->freeResult();
        yield 'SQL query executed successfully' . PHP_EOL;
    }

    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::validate()
     */
    public function validate(): \Generator
    {
        yield from [];
    }
    
    /**
     * 
     * @return string
     */
    protected function getSql() : string
    {
        return $this->sql ?? '';
    }
    
    /**
     * The SQL to run (supports multiple statements!)
     * 
     * Available placeholders:
     * 
     * - `[#from_object_address#]`
     * - `[#to_object_address#]`
     * - `[#step_run_uid#]`
     * - `[#previuos_step_run_uid#]`
     * - `[#increment_value#]`
     * 
     * @uxon-property sql
     * @uxon-type string
     * 
     * @param string $value
     * @return SQLRunner
     */
    protected function setSql(string $value) : SQLRunner
    {
        $this->sql = $value;
        return $this;
    }
    
    /**
     * 
     * @return SqlDataConnectorInterface
     */
    protected function getSqlConnection() : SqlDataConnectorInterface
    {
        return $this->getToObject()->getDataConnection();
    }
    
    /**
     * 
     * @param string $sql
     * @param string $stepRunUid
     * @param string $previousStepRunUid
     * @param string $incrementValue
     * 
     * @return string
     */
    protected function replacePlaceholders(string $sql, string $stepRunUid, string $previousStepRunUid = null, string $incrementValue = null) : string
    {
        return StringDataType::replacePlaceholders($sql, [
            'from_object_address' => $this->getFromObject()->getDataAddress(),
            'to_object_address' => $this->getToObject()->getDataAddress(),
            'step_run_uid' => $stepRunUid,
            'previuos_step_run_uid' => $previousStepRunUid ?? 'NULL',
            'increment_value' => $incrementValue ?? ''
        ]);
    }
}