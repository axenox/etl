<?php
namespace axenox\ETL\ETLPrototypes;

use exface\Core\CommonLogic\UxonObject;

/**
 * Creates and fills a MySQL table with dates for the to-object.
 * 
 * The table will look like this:
 * 
 * ```
 *  CREATE TABLE IF NOT EXISTS {$table} (
 *        id           INTEGER PRIMARY KEY,  -- year*10000+month*100+day
 *        date      DATE NOT NULL,
 *        year         INTEGER NOT NULL,
 *        month        INTEGER NOT NULL, -- 1 to 12
 *        day          INTEGER NOT NULL, -- 1 to 31
 *        quarter      INTEGER NOT NULL, -- 1 to 4
 *        week_no      INTEGER NOT NULL, -- 1 to 52/53
 *        weekday_no   INTEGER NOT NULL, -- 0 to 6
 *        weekend_flag TINYINT(1) DEFAULT '0',
 *        UNIQUE td_ymd_idx (year,month,day),
 *        UNIQUE td_dbdate_idx (date)
 *  ) Engine=InnoDB;
 * 
 * ```
 * 
 * You can override the column names via `column_names`.
 * 
 * The step will generate rows for a range defined via `days_back` and `days_forward`.
 * 
 * **NOTE:** this step requires the permission to create, execute and delete procedures
 * for the SQL user!
 *  
 * @author Andrej Kabachnik
 *
 */
class MySQLDateDimensionGenerator extends SQLRunner
{
    private $dropTable = false;
    
    private $tableName = null;
    
    private $daysBack = 0;
    
    private $daysForward = 365;
    
    private $columnNames = [];
    
    private $columnNamesDefaults = [
        'year' => 'year',
        'month' => 'month',
        'day' => 'day',
        'quarter' => 'quarter',
        'week_no' => 'week_no',
        'weekday_no' => 'weekday_no',
        'weekend_flag' => 'weekend_flag'
    ];
    
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
        
        $table = $this->getToObject()->getDataAddress();
        $cols = $this->getColumnNames();
        $from = $this->getStartDate()->format('Y-m-d');
        $to = $this->getEndDate()->format('Y-m-d');
        
        if ($this->getDropTable()) {
            $insert = 'INSERT';
            $prepare = "DROP TABLE IF EXISTS {$table};";
        } else {
            $insert = 'INSERT IGNORE';
            $prepare = '';
        }
        
        return <<<SQL

{$prepare}
CREATE TABLE IF NOT EXISTS {$table} (
        id                      INTEGER PRIMARY KEY,  -- year*10000+month*100+day
        date                 DATE NOT NULL,
        {$cols['year']}         INTEGER NOT NULL,
        {$cols['month']}        INTEGER NOT NULL, -- 1 to 12
        {$cols['day']}          INTEGER NOT NULL, -- 1 to 31
        {$cols['quarter']}      INTEGER NOT NULL, -- 1 to 4
        {$cols['week_no']}      INTEGER NOT NULL, -- 1 to 52/53
        {$cols['weekday_no']}   INTEGER NOT NULL, -- 0 to 6
        {$cols['weekend_flag']} TINYINT DEFAULT '0',
        UNIQUE td_ymd_idx ({$cols['year']},{$cols['month']},{$cols['day']}),
        UNIQUE td_dbdate_idx (date)
) Engine=InnoDB;

DROP PROCEDURE IF EXISTS fill_date_dimension;

delimiter //

CREATE PROCEDURE fill_date_dimension(IN startdate DATE,IN stopdate DATE)
BEGIN
    DECLARE currentdate DATE;
    SET currentdate = startdate;
    WHILE currentdate <= stopdate DO
        {$insert} INTO {$table} VALUES (
            YEAR(currentdate)*10000+MONTH(currentdate)*100 + DAY(currentdate),
            currentdate,
            YEAR(currentdate),
            MONTH(currentdate),
            DAY(currentdate),
            QUARTER(currentdate),
            WEEKOFYEAR(currentdate),
            WEEKDAY(currentdate),
            IF(WEEKDAY(currentdate) >= 5, 1, 0)
         );
        SET currentdate = ADDDATE(currentdate,INTERVAL 1 DAY);
    END WHILE;
END//

delimiter ;

CALL fill_date_dimension('{$from}','{$to}');
DROP PROCEDURE IF EXISTS fill_date_dimension;

SQL;
    }
    
    protected function getDropTable() : bool
    {
        return $this->dropTable;
    }
    
    /**
     * Set to TRUE to drop the table and recreate it completely
     * 
     * @uxon-property drop_table
     * @uxon-type boolean
     * @uxon-default false
     * 
     * @param bool $value
     * @return MySQLDateDimensionGenerator
     */
    public function setDropTable(bool $value) : MySQLDateDimensionGenerator
    {
        $this->dropTable = $value;
        return $this;
    }
    
    /**
     * 
     * @return string[]
     */
    protected function getColumnNames() : array
    {
        return array_merge($this->columnNamesDefaults, $this->columnNames);
    }
    
    /**
     * Names for the default columns
     * 
     * @uxon-property column_names
     * @uxon-type array
     * @uxon-template {"year":"","month":"","day":"","quarter":"","week_no":"","weekday_no":"","weekend_flag":""}
     * 
     * @param array $value
     * @return MySQLDateDimensionGenerator
     */
    public function setColumnNames($value) : MySQLDateDimensionGenerator
    {
        $uxon = UxonObject::fromAnything($value);
        $this->columnNames = $uxon->toArray();
        return $this;
    }
    
    /**
     * 
     * @return int
     */
    protected function getDaysBack() : int
    {
        return $this->daysBack;
    }
    
    /**
     * Number of dates to generate in the past
     * 
     * @uxon-property days_back
     * @uxon-type integer
     * @uxon-default 0
     * 
     * @param int $value
     * @return MySQLDateDimensionGenerator
     */
    public function setDaysBack(int $value) : MySQLDateDimensionGenerator
    {
        $this->daysBack = $value;
        return $this;
    }
    
    /**
     * 
     * @return int
     */
    protected function getDaysForward() : int
    {
        return $this->daysForward;
    }
    
    /**
     * Number of dates to generate in the future
     * 
     * @uxon-property days_forward
     * @uxon-type integer
     * @uxon-default 365
     * 
     * @param int $value
     * @return MySQLDateDimensionGenerator
     */
    public function setDaysForward(int $value) : MySQLDateDimensionGenerator
    {
        $this->daysForward = $value;
        return $this;
    }
    
    /**
     * 
     * @return \DateTime
     */
    protected function getStartDate() : \DateTime
    {
        $date = new \DateTime();
        $date->sub(new \DateInterval('P' . $this->getDaysBack() . 'D')); // P1D means a period of 1 day
        return $date;
    }
    
    /**
     * 
     * @return \DateTime
     */
    protected function getEndDate() : \DateTime
    {
        $date = new \DateTime();
        $date->add(new \DateInterval('P' . $this->getDaysForward() . 'D')); // P1D means a period of 1 day
        return $date;
    }
}