-- UP

ALTER TABLE `etl_flow`
	ADD COLUMN `scheduler_oid` BINARY(16) NULL DEFAULT NULL AFTER `app_oid`;
	
-- DOWN

ALTER TABLE `etl_flow`
	DROP COLUMN `scheduler_oid`;