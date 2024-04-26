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

/* Remove flow_oid from table etl_webservice */
/* Remove foreign key*/
set @var=if(
	(
		SELECT true 
			FROM information_schema.TABLE_CONSTRAINTS 
			WHERE CONSTRAINT_SCHEMA = DATABASE()
			    AND TABLE_NAME        = 'etl_webservice'
			    AND CONSTRAINT_NAME   = 'FK route to flow'
			    AND CONSTRAINT_TYPE   = 'FOREIGN KEY'
    ) = true,
    'ALTER TABLE etl_webservice DROP FOREIGN KEY `FK route to flow`',
    'SELECT \'Foreign key "FK route to flow" does not exist!\'');
prepare stmt from @var;
execute stmt;
deallocate prepare stmt;
/* Remove index */
set @var=if(
	(
		SELECT true 
			FROM information_schema.statistics 
  			WHERE table_schema = DATABASE() 
				AND TABLE_NAME = 'etl_webservice' 
				AND INDEX_NAME = 'FK route to flow'
	) = true,
    'ALTER TABLE etl_webservice DROP INDEX `FK route to flow`',
    'SELECT \'Index "FK route to flow" does not exist!\'');
prepare stmt from @var;
execute stmt;
deallocate prepare stmt;

CALL execute_sql_on_existing_column('etl_webservice', 'flow_oid', 'ALTER TABLE etl_webservice DROP COLUMN flow_oid');

-- DOWN
CALL execute_sql_on_missing_column('etl_webservice', 'flow_oid', 'ALTER TABLE etl_webservice ADD COLUMN flow_oid binary(16) DEFAULT NULL');

UPDATE `etl_webservice` w
    INNER JOIN `etl_webservice_flow` wf ON wf.`webservice_oid` = w.`oid`
SET w.`flow_oid` = wf.`flow_oid`;

DROP TABLE IF EXISTS `etl_webservice_flow`;

