-- UP
CALL execute_sql_on_missing_column('etl_step', 'step_flow_sequence', 'ALTER TABLE etl_step ADD COLUMN step_flow_sequence INT NOT NULL DEFAULT -1');

UPDATE 
	etl_step
	INNER JOIN (
		SELECT DISTINCT 
			flow_run_pos, 
			sr.flow_oid, 
			sr.step_oid 
		FROM etl_step_run sr
			LEFT JOIN etl_flow_run fr ON fr.oid = sr.flow_run_oid
		WHERE  fr.valid_flag = 1 
			AND fr.created_on = (
				SELECT MAX(etl_flow_run_tmp.created_on) 
					FROM (SELECT 
							r.flow_oid,
							MIN(r.created_on) AS created_on
						FROM etl_step_run r
						GROUP BY r.flow_run_oid, r.flow_oid
					) etl_flow_run_tmp
				WHERE etl_flow_run_tmp.flow_oid = fr.flow_oid
			)
  	) srs ON etl_step.oid = srs.step_oid 
  		AND etl_step.flow_oid = srs.flow_oid
SET step_flow_sequence = srs.flow_run_pos
WHERE step_flow_sequence IS NULL OR step_flow_sequence = -1;

/* Simple flows with only one step get the sequence 1 if not processed already */	
UPDATE etl_step
SET step_flow_sequence = 1
	WHERE (SELECT temp_s.`Count` FROM (SELECT COUNT(`oid`) AS `Count` FROM etl_step s1 WHERE s1.flow_oid = etl_step.flow_oid) AS temp_s) = 1
		AND step_flow_sequence IS NULL OR step_flow_sequence = -1;

/* All other steps get -1 to indicate, that they must be reviewed! */
UPDATE etl_step SET step_flow_sequence = -1 WHERE step_flow_sequence IS NULL;

ALTER TABLE etl_step ALTER step_flow_sequence DROP DEFAULT;

-- delete old value
CALL execute_sql_on_existing_column('etl_step', 'run_after_step_oid', 'ALTER TABLE etl_step DROP COLUMN run_after_step_oid');

-- DOWN

CALL execute_sql_on_missing_column('etl_step', 'run_after_step_oid', 'ALTER TABLE etl_step ADD COLUMN run_after_step_oid BINARY(16) NULL');

-- DON'T drop sequence column to preserve data!
