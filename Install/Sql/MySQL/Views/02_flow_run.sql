CREATE OR REPLACE VIEW etl_flow_run AS
SELECT 
	r.flow_run_oid AS oid,
	r.flow_oid,
	COUNT(1) AS steps_started,
	SUM(r.success_flag + r.error_flag) AS steps_run,
	SUM(r.error_flag) AS `errors`,
	SUM(r.step_disabled_flag) AS `steps_disabled`,
	SUM(r.skipped_flag) AS `steps_skipped`,
	SUM(r.invalidated_flag) AS `steps_invalidated`,
	SUM(
		CASE
			WHEN r.success_flag = 0 AND r.end_time IS NULL AND DATE_ADD(r.start_time, INTERVAL r.timeout_seconds SECOND) >= NOW() THEN 1 
			ELSE 0 
		END
	) AS steps_running,
	SUM(
		CASE
			WHEN r.success_flag = 0 AND r.end_time IS NULL AND DATE_ADD(r.start_time, INTERVAL r.timeout_seconds SECOND) < NOW() THEN 1 
			ELSE 0 
		END
	) AS steps_timed_out,
	IF (SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1), 1, 0) AS 'valid_flag',
	IF (SUM(r.success_flag) + SUM(r.step_disabled_flag) + SUM(r.skipped_flag) - SUM(r.invalidated_flag) = COUNT(1), 1, 0) * SUM(r.result_count) AS 'valid_rows',
	MIN(r.start_time) AS start_time,
	MAX(r.end_time) AS end_time,
	TIMESTAMPDIFF(SECOND, MIN(r.start_time), MAX(r.end_time)) AS duration_seconds,
	TIMEDIFF(MIN(r.start_time), MAX(r.end_time)) AS duration,
	MAX(r.error_message) AS error_message,
	MAX(r.error_log_id) AS error_log_id,
	MIN(r.created_on) AS created_on,
	MAX(r.modified_on) AS modified_on,
	MAX(r.created_by_user_oid) AS created_by_user_oid,
	MAX(r.modified_by_user_oid) AS modified_by_user_oid
FROM etl_step_run r
GROUP BY r.flow_run_oid, r.flow_oid;