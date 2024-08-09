-- UP

-- Make version columns longer
CALL execute_sql_on_existing_column('etl_webservice', 'version', 'CHANGE COLUMN `version` `version` VARCHAR(50) NULL DEFAULT NULL');
CALL execute_sql_on_existing_column('etl_flow', 'version', 'CHANGE COLUMN `version` `version` VARCHAR(50) NULL DEFAULT NULL');

-- Remove request_direction from web services
CALL execute_sql_on_existing_column('etl_webservice', 'request_direction', 'ALTER TABLE etl_webservice DROP COLUMN request_direction');

-- Make local url required
CALL execute_sql_on_existing_column('etl_webservice', 'local_url', 'ALTER TABLE `etl_webservice` CHANGE COLUMN `local_url` `local_url` VARCHAR(400) NOT NULL');

ALTER TABLE `etl_webservice`
	ADD UNIQUE INDEX `Unique per path and version` (`local_url`, `version`);
	
ALTER TABLE `etl_flow`
	ADD UNIQUE INDEX `Unique per alias and version` (`alias`, `version`);
	
-- DOWN
ALTER TABLE `etl_webservice`
	DROP INDEX `Unique per path and version`;
ALTER TABLE `etl_flow`
	DROP INDEX `Unique per alias and version`;

	
CALL execute_sql_on_existing_column('etl_webservice', 'version', 'CHANGE COLUMN `version` `version` VARCHAR(10) NULL');
CALL execute_sql_on_existing_column('etl_flow', 'version', 'CHANGE COLUMN `version` `version` VARCHAR(10) NULL');

CALL execute_sql_on_existing_column('etl_webservice', 'local_url', 'CHANGE COLUMN `local_url` `local_url` VARCHAR(400) NULL');

CALL execute_sql_on_missing_column('etl_webservice', 'request_direction', 'ALTER TABLE etl_webservice ADD request_direction varchar(10) NOT NULL DEFAULT \'Inbound\'');