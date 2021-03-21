-- UP

ALTER TABLE `etl_step_run`
	CHANGE COLUMN `end_increment_value` `result_uxon` TEXT NULL COLLATE 'utf8_general_ci' AFTER `end_time`,
	ADD COLUMN `result_uxon_of_prev_run` TEXT NULL DEFAULT NULL AFTER `end_time`,
	ADD COLUMN `result_count` INT NOT NULL DEFAULT '0' AFTER `end_time`;
	
-- DOWN

ALTER TABLE `etl_step_run`
	CHANGE COLUMN `result_uxon` `end_increment_value` VARCHAR(200) NULL COLLATE 'utf8_general_ci' AFTER `end_time`,
	DROP COLUMN `result_uxon_of_prev_run`,
	DROP COLUMN `result_count`;