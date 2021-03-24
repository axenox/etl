<?php
namespace axenox\ETL\Common\Traits;

use axenox\ETL\Interfaces\ETLStepInterface;

trait IncrementalSqlWhereTrait
{
    private $incrementalWhere = null;
    
    protected function getIncrementalWhere() : ?string
    {
        return $this->incrementalWhere;
    }
    
    /**
     * SQL predicate for the WHERE statement that will take care of the `[#last_step_increment_value#]` placeholder.
     *
     * @uxon-property incremental_where
     * @uxon-type string
     *
     * @param string $value
     * @return ETLStepInterface
     */
    protected function setIncrementalWhere(string $value) : ETLStepInterface
    {
        $this->incrementalWhere = $value;
        return $this;
    }
}