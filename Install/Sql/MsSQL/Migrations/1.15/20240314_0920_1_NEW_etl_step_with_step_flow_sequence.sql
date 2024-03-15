-- UP
IF COL_LENGTH('dbo.etl_step','step_flow_sequence') IS NULL
BEGIN
    ALTER TABLE [dbo].[etl_step] ADD [step_flow_sequence] INT NULL;
    
    /* Get the sequence from last successful run */
    UPDATE etl_step SET step_flow_sequence = (
    	SELECT
    		srl.flow_run_pos
    	FROM 
			(
				SELECT TOP 1
					r.flow_run_oid AS oid,
					r.flow_oid,
					(CASE 
						WHEN SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1) THEN 1 
						ELSE 0 
					END) AS 'valid_flag',
					MIN(r.created_on) AS created_on
				FROM [dbo].[etl_step_run] r
					WHERE r.flow_oid = etl_step.flow_oid
				GROUP BY r.flow_run_oid, r.flow_oid
				HAVING (CASE 
						WHEN SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1) THEN 1 
						ELSE 0 
					END) = 1
				ORDER BY MIN(r.created_on) DESC
			) frl
			LEFT JOIN etl_step_run srl ON srl.flow_run_oid = frl.oid
			WHERE srl.step_oid = etl_step.oid
		)
		WHERE step_flow_sequence IS NULL OR step_flow_sequence = -1;
	
	/* Simple flows with only one step get the sequence 1 if not processed already */	
	UPDATE etl_step SET step_flow_sequence = 1 
		WHERE (SELECT COUNT(*) FROM etl_step s1 WHERE s1.flow_oid = etl_step.flow_oid) = 1
			AND step_flow_sequence IS NULL OR step_flow_sequence = -1;
	
	/* All other steps get -1 to indicate, that they must be reviewed! */
	UPDATE etl_step SET step_flow_sequence = -1 WHERE step_flow_sequence IS NULL;

    DECLARE @alterQuery NVARCHAR(MAX);
    SET @alterQuery = 'ALTER TABLE [dbo].[etl_step] ALTER COLUMN [step_flow_sequence] INT NOT NULL;'
    EXEC sp_executesql @alterQuery;
END

IF COL_LENGTH('dbo.etl_step','run_after_step_oid') IS NOT NULL
ALTER TABLE [dbo].[etl_step] DROP COLUMN [run_after_step_oid];

-- DOWN

IF COL_LENGTH('dbo.etl_step','run_after_step_oid') IS NULL
ALTER TABLE [dbo].[etl_step] ADD [run_after_step_oid] BINARY(16) NULL;

-- cannot change back structural changes