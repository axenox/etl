--
-- Table structure for table `etl_flow`
--

CREATE TABLE IF NOT EXISTS `etl_flow` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `name` varchar(50) NOT NULL,
  `alias` varchar(50) NOT NULL,
  `description` text,
  `app_oid` binary(16) DEFAULT NULL,
  PRIMARY KEY (`oid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Table structure for table `etl_run`
--

CREATE TABLE IF NOT EXISTS `etl_run` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `step_oid` binary(16) NOT NULL,
  `flow_oid` binary(16) NOT NULL,
  `flow_run_oid` binary(16) NOT NULL,
  `flow_run_pos` int(11) NOT NULL,
  `start_time` datetime NOT NULL,
  `timeout_seconds` int(11) NOT NULL,
  `end_time` datetime DEFAULT NULL,
  `end_increment_value` varchar(200) DEFAULT NULL,
  `output` longtext,
  `step_disabled_flag` tinyint(1) NOT NULL DEFAULT '0',
  `success_flag` tinyint(1) NOT NULL DEFAULT '0',
  `skipped_flag` tinyint(1) NOT NULL DEFAULT '0',
  `invalidated_flag` tinyint(1) NOT NULL DEFAULT '0',
  `error_flag` tinyint(1) NOT NULL DEFAULT '0',
  `error_message` varchar(200) DEFAULT NULL,
  `error_log_id` varchar(10) DEFAULT NULL,
  `error_widget` longtext,
  PRIMARY KEY (`oid`) USING BTREE,
  UNIQUE KEY `Flow run pos unique per run` (`flow_run_oid`,`flow_run_pos`) USING BTREE,
  KEY `Flow run` (`flow_run_oid`,`flow_oid`,`start_time`,`end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Table structure for table `etl_step`
--

CREATE TABLE IF NOT EXISTS `etl_step` (
  `oid` binary(16) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) NOT NULL,
  `modified_by_user_oid` binary(16) NOT NULL,
  `name` varchar(50) NOT NULL,
  `type` char(1) NOT NULL,
  `description` text,
  `flow_oid` binary(16) NOT NULL,
  `from_object_oid` binary(16) DEFAULT NULL,
  `to_object_oid` binary(16) NOT NULL,
  `etl_prototype_path` varchar(250) NOT NULL,
  `etl_config_uxon` longtext,
  `disabled` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`oid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;