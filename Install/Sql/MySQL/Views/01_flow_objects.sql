CREATE OR REPLACE VIEW etl_flow_objects AS
SELECT
	l2.flow_oid,
	l2.object_oid,
	(CASE
		WHEN l2.out_e > 0 THEN 'E'
		WHEN l2.in_l > 0 THEN 'L'
		WHEN l2.out_t > 0 AND l2.in = 0 THEN 'L'
		ELSE 'T'
	END) AS `type`,
	(CASE
		WHEN l2.out_e > 0 THEN 1
		WHEN l2.in_l > 0 THEN 3
		WHEN l2.out_t > 0 AND l2.in = 0 THEN 3
		ELSE 2
	END) AS `level`
FROM
	(
		SELECT 
			l1.flow_oid,
			l1.object_oid,
			SUM(`in`) AS 'in',
			SUM(`out`) AS 'out',
			SUM(in_e) AS in_e,
			SUM(out_e) AS out_e,
			SUM(in_t) AS in_t,
			SUM(out_t) AS out_t,
			SUM(in_l) AS in_l,
			SUM(out_l) AS out_l
		FROM
			(
				(
					SELECT
						from_object_oid AS object_oid,
						flow_oid,
						0 AS 'in',
						1 AS 'out',
						0 AS 'in_e',
						1 AS 'out_e',
						0 AS 'in_t',
						0 AS 'out_t',
						0 AS 'in_l',
						0 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'E'
						AND from_object_oid IS NOT NULL
				) 
				UNION ALL
				(
					SELECT
						to_object_oid AS object_oid,
						flow_oid,	
						1 AS 'in',
						0 AS 'out',					
						1 AS 'in_e',
						0 AS 'out_e',
						0 AS 'in_t',
						0 AS 'out_t',
						0 AS 'in_l',
						0 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'E'
						AND to_object_oid IS NOT NULL
				) 
				UNION ALL
				(
					SELECT
						from_object_oid AS object_oid,
						flow_oid,
						0 AS 'in',
						1 AS 'out',
						0 AS 'in_e',
						0 AS 'out_e',
						0 AS 'in_t',
						1 AS 'out_t',
						0 AS 'in_l',
						0 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'T'
						AND from_object_oid IS NOT NULL
				) 
				UNION ALL
				(
					SELECT
						to_object_oid AS object_oid,
						flow_oid,	
						1 AS 'in',
						0 AS 'out',	
						0 AS 'in_e',
						0 AS 'out_e',
						1 AS 'in_t',
						0 AS 'out_t',
						0 AS 'in_l',
						0 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'T'
						AND to_object_oid IS NOT NULL
				) 
				UNION ALL
				(
					SELECT
						from_object_oid AS object_oid,
						flow_oid,
						0 AS 'in',
						1 AS 'out',
						0 AS 'in_e',
						0 AS 'out_e',
						0 AS 'in_t',
						0 AS 'out_t',
						0 AS 'in_l',
						1 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'L'
						AND from_object_oid IS NOT NULL
				) 
				UNION ALL
				(
					SELECT
						to_object_oid AS object_oid,
						flow_oid,	
						1 AS 'in',
						0 AS 'out',	
						0 AS 'in_e',
						0 AS 'out_e',
						0 AS 'in_t',
						0 AS 'out_t',
						1 AS 'in_l',
						0 AS 'out_l'
					FROM
						etl_step es
					WHERE type = 'L'
						AND to_object_oid IS NOT NULL
				) 
			) l1
		GROUP BY l1.flow_oid, l1.object_oid
	) l2