-- UP

ALTER TABLE `etl_step`
	ADD COLUMN `stop_flow_on_error` TINYINT(1) NOT NULL DEFAULT '1' AFTER `disabled`,
	ADD COLUMN `run_after_step_oid` BINARY(16) NULL DEFAULT NULL AFTER `stop_flow_on_error`;
	
-- DOWN

ALTER TABLE `etl_step`
	DROP COLUMN `stop_flow_on_error`,
	DROP COLUMN `run_after_step_oid`;