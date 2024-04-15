-- UP
CREATE TABLE IF NOT EXISTS `etl_webservice_flow`(
    `oid` binary(16) NOT NULL,
    `created_on` datetime NOT NULL,
    `modified_on` datetime NOT NULL,
    `created_by_user_oid` binary(16) NOT NULL,
    `modified_by_user_oid` binary(16) NOT NULL,
    `webservice_oid` binary(16) NOT NULL,
    `flow_oid` binary(16) NOT NULL,
    `route` NVARCHAR(30),
    CONSTRAINT `FK route to flow` FOREIGN KEY (`flow_oid`) REFERENCES `etl_flow` (`oid`),
    CONSTRAINT `FK flow to route` FOREIGN KEY (`webservice_oid`) REFERENCES `etl_webservice` (`oid`));

INSERT INTO `etl_webservice_flow`
SELECT UNHEX(REPLACE(UUID(), '-', '')), CURDATE(), CURDATE(), created_by_user_oid, created_by_user_oid, oid, flow_oid, NULL FROM etl_webservice w
WHERE flow_oid IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM etl_webservice_flow wf
    WHERE wf.webservice_oid = w.oid
);

CALL execute_sql_on_existing_column('etl_webservice', 'flow_oid', 'ALTER TABLE etl_webservice DROP COLUMN flow_oid');

-- DOWN
CALL execute_sql_on_missing_column('etl_webservice', 'flow_oid', 'ALTER TABLE etl_webservice ADD COLUMN flow_oid binary(16) DEFAULT NULL');

UPDATE `etl_webservice` w
    INNER JOIN `etl_webservice_flow` wf ON wf.`webservice_oid` = w.`oid`
SET w.`flow_oid` = wf.`flow_oid`;

DROP TABLE IF EXISTS `etl_webservice_flow`;

