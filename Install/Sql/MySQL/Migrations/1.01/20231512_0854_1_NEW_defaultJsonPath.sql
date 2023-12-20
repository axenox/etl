-- UP
SET @dbname = DATABASE();
SET @tablename = "etl_webservice_type";
SET @columnname = "default_response_path";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  "SELECT 1",
  CONCAT("ALTER TABLE ", @tablename, " ADD ", @columnname, " text COLLATE 'utf8mb3_general_ci' NULL")
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- DOWN
SET @dbname = DATABASE();
SET @tablename = "etl_webservice_type";
SET @columnname = "default_response_path";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  "SELECT 1",
  CONCAT("ALTER TABLE ", @tablename, " DROP COLUMN ", @columnname)
));
PREPARE deleteIfNotExists FROM @preparedStatement;
EXECUTE deleteIfNotExists;
DEALLOCATE PREPARE deleteIfNotExists;