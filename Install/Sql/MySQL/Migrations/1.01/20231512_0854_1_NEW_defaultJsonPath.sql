-- UP
ALTER TABLE `etl_webservice_type`
ADD `default_response_path` text COLLATE 'utf8mb3_general_ci' NULL;

-- DOWN

ALTER TABLE `etl_webservice_type`
DROP COLUMN `default_response_path`;
