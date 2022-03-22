IF OBJECT_ID('dbo.etl_flow_run_latest', 'V') IS NOT NULL
    DROP VIEW [dbo].[etl_flow_run_latest];
GO

CREATE VIEW etl_flow_run_latest (
	flow_oid,
	flow_run_oid
) AS
	SELECT 
		r.flow_oid,
		MAX(r.flow_run_oid) AS flow_run_oid
	FROM (
		SELECT 
				r1.flow_oid,
				MAX(r1.start_time) AS start_time
			FROM [dbo].[etl_step_run] r1 
			GROUP BY r1.flow_oid
		) r2 
		INNER JOIN [dbo].[etl_step_run] r ON r.flow_oid = r2.flow_oid AND r.start_time = r2.start_time
	GROUP BY r.flow_oid
;