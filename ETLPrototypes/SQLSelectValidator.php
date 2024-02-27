<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\DataQueries\SqlDataQuery;
use axenox\ETL\Common\Traits\SqlIncrementalWhereTrait;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use axenox\ETL\Interfaces\ETLStepDataInterface;

/**
 * Performs a SELECT * query on the to-object to check for errors.
 * 
 * This prototype is mainly usefull to check views for data inconsistencies, calculation errors, etc. 
 * 
 * To avoid reading the entire table or view, make the step incremental by adding `incremental_where` 
 * and `sql_to_get_current_increment_value` along with placeholders like `[#last_step_increment_value#]` 
 * to read only entries added after the last run of this step.
 * 
 * By defualt this step will execute the followin SQL statement on the data source of the to-object:
 * 
 * ```
 *  SELECT * FROM [#to_object_address#] WHERE [#incremental_where#];
 * 
 * ```
 * 
 * You can override it by setting the `sql` property.
 * 
 * @author Andrej Kabachnik
 *
 */
class SQLSelectValidator extends SQLRunner
{
    use SqlIncrementalWhereTrait;
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getSql()
     */
    protected function getSql() : string
    {
        if ($customSql = parent::getSql()) {
            return $customSql;
        }
        
        return <<<SQL

SELECT * FROM [#to_object_address#] WHERE [#incremental_where#];

SQL;
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::countAffectedRows()
     */
    protected function countAffectedRows(SqlDataQuery $query) : ?int
    {
        return count($query->getResultArray());
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \axenox\ETL\ETLPrototypes\SQLRunner::getPlaceholders()
     */
    protected function getPlaceholders(ETLStepDataInterface $stepData) : array
    {
        return array_merge(parent::getPlaceholders($stepData), [
            'incremental_where' => $this->getSqlIncrementalWhere() ?? '(1=1)'
        ]);
    }      
}