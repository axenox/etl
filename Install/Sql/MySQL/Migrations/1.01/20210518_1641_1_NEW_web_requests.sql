-- UP

-- exface.etl_webservice_route
CREATE TABLE IF NOT EXISTS `etl_webservice_route` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `alias` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `app_oid` binary(16) DEFAULT NULL,
  `description` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `direction` varchar(3) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'IN',
  `flow_oid` binary(16) NOT NULL,
  `in_url` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `out_connection_oid` binary(16) DEFAULT NULL,
  `config_uxon` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  PRIMARY KEY (`oid`) USING BTREE,
  KEY `FK route to flow` (`flow_oid`),
  CONSTRAINT `FK route to flow` FOREIGN KEY (`flow_oid`) REFERENCES `etl_flow` (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- exface.etl_webservice_request
CREATE TABLE IF NOT EXISTS `etl_webservice_request` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `route_oid` binary(16) DEFAULT NULL,
  `flow_run_uid` binary(16) DEFAULT NULL,
  `url` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `url_path` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `http_method` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `http_response_code` smallint DEFAULT NULL,
  `http_headers` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `http_body` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  `http_content_type` varchar(200) DEFAULT NULL,
  `status` tinyint NOT NULL DEFAULT '10',
  `result_text` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  `error_message` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  `error_logid` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`oid`) USING BTREE,
  KEY `FK request to route` (`route_oid`),
  CONSTRAINT `FK request to route` FOREIGN KEY (`route_oid`) REFERENCES `etl_webservice_route` (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
	
-- DOWN

/* Do not drop anything to keep historical data */