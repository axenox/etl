-- UP

ALTER TABLE `etl_step_run`
	ADD COLUMN `incremental_flag` TINYINT(1) NOT NULL DEFAULT '0' AFTER `output`,
	ADD COLUMN `incremental_after_run_oid` BINARY(16) NULL AFTER `incremental_flag`,
	DROP COLUMN `result_uxon_of_prev_run`,
	ADD COLUMN `debug_widget` LONGTEXT NULL DEFAULT NULL AFTER `start_time`;
	
-- DOWN

ALTER TABLE `etl_step_run`
	DROP COLUMN `incremental_after_run_oid`,
	DROP COLUMN `incremental_flag`,´
	DROP COLUMN `debug_widget`,
	ADD COLUMN `result_uxon_of_prev_run` TEXT NULL AFTER `result_count`;