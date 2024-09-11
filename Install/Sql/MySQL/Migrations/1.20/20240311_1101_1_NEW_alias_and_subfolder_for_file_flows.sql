-- UP

ALTER TABLE `etl_file_flow`
ADD `subfolder` varchar(255) NULL;

ALTER TABLE `etl_file_flow`
ADD `alias` varchar(100) NOT NULL;

ALTER TABLE `etl_file_upload`
CHANGE `file_flow_oid` `file_flow_oid` binary(16) NULL;

ALTER TABLE `etl_file_upload`
ADD FOREIGN KEY `fk_file_flow_oid` (`file_flow_oid`) REFERENCES `etl_file_flow` (`oid`) ON DELETE SET NULL ON UPDATE RESTRICT;
	
-- DOWN

ALTER TABLE `etl_file_flow`
DROP COLUMN `subfolder`;

ALTER TABLE `etl_file_flow`
DROP COLUMN `alias`;

ALTER TABLE `etl_file_upload`
DROP FOREIGN KEY `fk_file_flow_oid`;

ALTER TABLE `etl_file_upload`
CHANGE `file_flow_oid` `file_flow_oid` binary(16) NOT NULL;