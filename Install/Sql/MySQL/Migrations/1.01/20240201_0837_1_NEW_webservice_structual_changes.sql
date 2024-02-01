-- UP

-- rename table
SET @dbname = DATABASE();
SET @tablenameOld = "etl_webservice_route";
SET @tablename = "etl_webservice";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablenameOld)
      AND (table_schema = @dbname)
  ) > 0,
  CONCAT("ALTER TABLE ", @tablenameOld, " RENAME TO `etl_webservice`;"),
  "SELECT 1"
));

PREPARE renameTableIfOldName FROM @preparedStatement;
EXECUTE renameTableIfOldName;
DEALLOCATE PREPARE renameTableIfOldName;

-- change url column
SET @columnname = "in_url";
SET @newColumnname = "local_url";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  CONCAT("ALTER TABLE ", @tablename, " CHANGE ", @columnname, " " , @newColumnname, " varchar(400) COLLATE 'utf8mb3_general_ci' NULL; "),
  "SELECT 1"
));

PREPARE alterColumnIfOldColumn FROM @preparedStatement;
EXECUTE alterColumnIfOldColumn;
DEALLOCATE PREPARE alterColumnIfOldColumn;

-- change connection column
SET @columnname = "out_connection_oid";
SET @newColumnname = "remote_connection_oid";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  CONCAT("ALTER TABLE ", @tablename, " CHANGE ", @columnname, " " , @newColumnname, " binary(16) NULL; "),
  "SELECT 1"
));

PREPARE renameColumnIfOldName FROM @preparedStatement;
EXECUTE renameColumnIfOldName;
DEALLOCATE PREPARE renameColumnIfOldName;

-- Change direction to request direction
CREATE PROCEDURE change_current_direction()
BEGIN
    SET @dbname = DATABASE();
    SET @tablename = "etl_webservice";
    SET @columnname = "direction";
    SET @newColumnname = "request_direction";

    -- Set your condition here
    SET @needsAction = (SELECT IF(
        (
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
            (table_name = @tablename)
            AND (table_schema = @dbname)
            AND (column_name = @columnname)
        ) > 0, TRUE, FALSE));

    -- Check the condition and execute statements accordingly
    IF @needsAction THEN
        -- change column name and type
        SET @alterQuery = CONCAT('ALTER TABLE ', @tablename, " CHANGE ", @columnname, " " , @newColumnname, " varchar(10) COLLATE 'utf8mb3_general_ci' NOT NULL DEFAULT 'Inbound' AFTER `description`;");
        PREPARE stmt FROM @alterQuery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- change current values
        SET @updateQuery1 = CONCAT("UPDATE ", @tablename, " SET " , @newColumnname, " = 'Inbound' WHERE " , @newColumnname, " = 'IN'; ");
        SET @updateQuery2 = CONCAT("UPDATE ", @tablename, " SET " , @newColumnname, " = 'Outbound' WHERE " , @newColumnname, " = 'OUT'; ");
        
        PREPARE stmt FROM @updateQuery1;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        PREPARE stmt FROM @updateQuery2;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END;

CALL change_current_direction();
DROP PROCEDURE IF EXISTS change_current_direction;

-- add flow direction
CREATE PROCEDURE new_flow_direction()
BEGIN
    SET @dbname = DATABASE();
    SET @tablename = "etl_webservice";
    SET @columnname = "flow_direction";

    -- Set your condition here
    SET @needsAction = (SELECT IF(
        (
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
            (table_name = @tablename)
            AND (table_schema = @dbname)
            AND (column_name = @columnname)
        ) > 0, FALSE, TRUE));

    -- Check the condition and execute statements accordingly
    IF @needsAction THEN
        -- add column
        SET @alterQuery = CONCAT('ALTER TABLE ', @tablename, ' ADD ', @columnname, ' varchar(3) COLLATE utf8mb3_general_ci NULL AFTER `flow_oid`');
        PREPARE stmt FROM @alterQuery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- fill with values
        SET @updateQuery1 = CONCAT('UPDATE ', @tablename, ' SET ', @columnname, " = 'IN' WHERE `request_direction` = 'Inbound'");
        SET @updateQuery2 = CONCAT('UPDATE ', @tablename, ' SET ', @columnname, " = 'OUT' WHERE `request_direction` = 'Outbound'");
        
        PREPARE stmt FROM @updateQuery1;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        PREPARE stmt FROM @updateQuery2;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- make not nullable
        SET @alterTableQuery = CONCAT('ALTER TABLE ', @tablename, ' MODIFY ', @columnname, ' varchar(3) COLLATE utf8mb3_general_ci NULL');
        PREPARE stmt FROM @alterTableQuery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END;

CALL new_flow_direction();
DROP PROCEDURE IF EXISTS new_flow_direction;

-- DOWN

-- no changing back to schema