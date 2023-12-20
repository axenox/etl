-- UP
SET @dbname = DATABASE();
SET @tablename = "etl_webservice_route";
SET @columnname = "swagger_json";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  "SELECT 1",
  CONCAT("ALTER TABLE ", @tablename, " ADD ", @columnname, " longtext COLLATE 'utf8mb3_general_ci' NULL")
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- DOWN
SET @dbname = DATABASE();
SET @tablename = "etl_webservice_route";
SET @columnname = "swagger_json";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  CONCAT("ALTER TABLE ", @tablename, " DROP COLUMN ", @columnname),
  "SELECT 1"
));
PREPARE deleteIfNotExists FROM @preparedStatement;
EXECUTE deleteIfNotExists;
DEALLOCATE PREPARE deleteIfNotExists;
