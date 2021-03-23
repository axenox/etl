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
    
    private $wrapInTransaction = true;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(string $stepRunUid, string $previousStepRunUid = null, ETLStepResultInterface $lastResult = null) : \Generator
    {
        $sql = $this->getSql();
        $sql = StringDataType::replacePlaceholders($sql, $this->getPlaceholders($stepRunUid, $lastResult));
        $connection = $this->getSqlConnection();
        
        if ($this->isWrappedInTransaction()) {
            $connection->transactionStart();
        }
        $query = $connection->runSql($sql, true);
        if ($this->isWrappedInTransaction()) {
            $connection->transactionCommit();
        }
        
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
     * - `[#last_run_uid#]`
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
    
    protected function getPlaceholders(string $stepRunUid, ETLStepResultInterface $lastResult = null) : array
    {
        return array_merge(parent::getPlaceholders($stepRunUid, $lastResult),[
            'from_object_address' => $this->getFromObject()->getDataAddress(),
            'to_object_address' => $this->getToObject()->getDataAddress()
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
    
    protected function isWrappedInTransaction() : bool
    {
        return $this->wrapInTransaction;
    }
    
    /**
     * Set to FALSE to skip starting/committing a transaction before/after the SQL query.
     * 
     * Disabling the transaction may be usefull if transaction handling is done by the query
     * itself (if supported by the database).
     * 
     * @uxon-property wrap_in_transaction
     * @uxon-type boolean
     * @uxon-default true
     * 
     * @param bool $value
     * @return SQLRunner
     */
    public function setWrapInTransaction(bool $value) : SQLRunner
    {
        $this->wrapInTransaction = $value;
        return $this;
    }
}