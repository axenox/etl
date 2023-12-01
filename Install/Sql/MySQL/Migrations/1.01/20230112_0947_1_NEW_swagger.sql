-- UP

ALTER TABLE `etl_webservice_route`
ADD `swagger_json` longtext COLLATE 'utf8mb3_general_ci' NULL;

-- DOWN

ALTER TABLE `etl_webservice_route`
DROP COLUMN `swagger_json`;
