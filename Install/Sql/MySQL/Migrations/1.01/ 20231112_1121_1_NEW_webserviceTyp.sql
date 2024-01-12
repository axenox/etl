-- UP

CREATE TABLE IF NOT EXISTS `etl_webservice_type` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `name` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `version` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `app_oid` binary(16) DEFAULT NULL,
  `schema_json` longtext CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  PRIMARY KEY (`oid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC;

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_route";
SET @columnname = "type_oid";
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (table_name = @tablename)
      AND (table_schema = @dbname)
      AND (column_name = @columnname)
  ) > 0,
  "SELECT 1",
  CONCAT("ALTER TABLE ", @tablename, " ADD ", @columnname, " binary(16) NULL")
));

-- store validation response
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_request";
SET @columnname = "response_body";
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

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_request";
SET @columnname = "response_header";
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

DROP TABLE IF EXISTS `etl_webservice_type`;

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_route";
SET @columnname = "type_oid";
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
PREPARE deleteIfExists FROM @preparedStatement;
EXECUTE deleteIfExists;
DEALLOCATE PREPARE deleteIfExists;

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_request";
SET @columnname = "response_body";
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
PREPARE deleteIfExists FROM @preparedStatement;
EXECUTE deleteIfExists;
DEALLOCATE PREPARE deleteIfExists;

SET @dbname = DATABASE();
SET @tablename = "etl_webservice_request";
SET @columnname = "response_header";
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
PREPARE deleteIfExists FROM @preparedStatement;
EXECUTE deleteIfExists;
DEALLOCATE PREPARE deleteIfExists;