CREATE OR REPLACE VIEW etl_flow_run AS
SELECT 
	r.flow_run_uid AS oid,
	r.flow_oid,
	COUNT(1) AS steps_run,
	SUM(r.error_flag) AS `errors`,
	SUM(r.step_disabled_flag) AS `disabled_steps`,
	SUM(r.skipped_flag) AS `skipped_steps`,
	SUM(r.invalidated_flag) AS `invalidated_steps`,
	SUM(
		CASE
			WHEN DATE_ADD(r.start_time, INTERVAL r.timeout_seconds SECOND) < NOW() THEN 1 
			ELSE 0 
		END
	) AS timeouts,
	MIN(r.start_time) AS start_time,
	MAX(r.end_time) AS end_time
FROM etl_run r
GROUP BY r.flow_run_uid, r.flow_oid;