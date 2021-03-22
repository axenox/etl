<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractETLPrototype;
use exface\Core\Interfaces\DataSources\SqlDataConnectorInterface;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\CommonLogic\DataQueries\SqlDataQuery;

class SQLRunner extends AbstractETLPrototype
{
    private $sql = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, string $previousStepRunUid = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $sql = $this->getSql();
        $sql = $this->replacePlaceholders($sql, $stepRunUid, $previousStepRunUid);
        $connection = $this->getSqlConnection();
        $query = $connection->runSql($sql, true);
        $cnt = $this->countAffectedRows($query);
        $query->freeResult();
        yield 'SQL query successful (' . $cnt . ' affected rows)' . PHP_EOL;
        $result = new IncrementalEtlStepResult($stepRunUid);
        if ($cnt !== null) {
            $result->setProcessedRowsCounter($cnt);
        }
        return $result;
    }
    
    /**
     * 
     * @param SqlDataQuery $query
     * @return int|NULL
     */
    protected function countAffectedRows(SqlDataQuery $query) : ?int
    {
        return $query->countAffectedRows();
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
            'previuos_step_run_uid' => $previousStepRunUid ?? 'NULL'
        ]);
    }
    
    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        return new IncrementalEtlStepResult($stepRunUid, $resultData);
    }
}