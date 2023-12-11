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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC

ALTER TABLE `etl_webservice_route`
ADD `type_oid` binary(16) NULL;

-- for validation of schema:
ALTER TABLE `etl_webservice_request`
ADD `response_body` text COLLATE 'utf8mb3_general_ci' NULL,
ADD `response_header` text COLLATE 'utf8mb3_general_ci' NULL AFTER `response_body`;

-- DOWN

DROP TABLE `etl_webservice_type`

ALTER TABLE `etl_webservice_route`
DROP COLUMN `type_oid`;
