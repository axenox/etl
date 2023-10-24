IF OBJECT_ID('dbo.etl_flow_run', 'V') IS NOT NULL
    DROP VIEW [dbo].[etl_flow_run];
GO

CREATE VIEW [dbo].[etl_flow_run] (
	oid,
	flow_oid,
	steps_started,
	steps_run,
	errors,
	steps_disabled,
	steps_skipped,
	steps_invalidated,
	steps_timed_out,
	valid_flag,
	valid_rows,
	start_time,
	end_time,
	duration_seconds,
	duration,
	error_message,
	error_log_id,
	created_on,
	modified_on,
	created_by_user_oid,
	modified_by_user_oid
) AS
	SELECT 
		r.flow_run_oid AS oid,
		r.flow_oid,
		COUNT(1) AS steps_started,
		SUM(r.success_flag + r.error_flag) AS steps_run,
		SUM(r.error_flag) AS errors,
		SUM(r.step_disabled_flag) AS steps_disabled,
		SUM(r.skipped_flag) AS steps_skipped,
		SUM(r.invalidated_flag) AS steps_invalidated,
		SUM(
			CASE
				WHEN r.success_flag = 0 AND r.end_time IS NULL AND DATEADD(second, r.timeout_seconds, r.start_time) < GETDATE() THEN 1 
				ELSE 0 
			END
		) AS steps_timed_out,
		(CASE 
			WHEN SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1) THEN 1 
			ELSE 0 
		END) AS 'valid_flag',
		(CASE 
			WHEN SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1) THEN 1 
			ELSE 0 
		END) * SUM(r.result_count) AS 'valid_rows',
		MIN(r.start_time) AS start_time,
		MAX(r.end_time) AS end_time,
		DATEDIFF(SECOND, MIN(r.start_time), MAX(r.end_time)) AS duration_seconds,
		CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, MIN(r.start_time), MAX(r.end_time)), 0), 108) AS duration,
		MAX(r.error_message) AS error_message,
		MAX(r.error_log_id) AS error_log_id,
		MIN(r.created_on) AS created_on,
		MAX(r.modified_on) AS modified_on,
		MAX(r.created_by_user_oid) AS created_by_user_oid,
		MAX(r.modified_by_user_oid) AS modified_by_user_oid
	FROM [dbo].[etl_step_run] r
	GROUP BY r.flow_run_oid, r.flow_oid
;