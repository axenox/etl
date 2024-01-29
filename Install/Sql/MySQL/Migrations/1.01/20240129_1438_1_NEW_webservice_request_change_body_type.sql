-- UP

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_request";
SET @columnname = "response_body";
SET @preparedStatement = (SELECT IF(
  (
    SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
  	WHERE table_name = @tablename
      AND column_name = @columnname
      AND table_schema = @dbname
  ) = 'text',
  CONCAT("ALTER TABLE ", @tablename, " CHANGE ", @columnname, " " , @columnname, " longtext COLLATE 'utf8mb3_general_ci' NULL"),
  "SELECT 1"
));

PREPARE alterColumnIfWrongType FROM @preparedStatement;
EXECUTE alterColumnIfWrongType;
DEALLOCATE PREPARE alterColumnIfWrongType;

-- DOWN

-- no changing back to old type