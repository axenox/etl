CREATE OR REPLACE VIEW etl_flow_objects AS
(
	SELECT 
		se.from_object_oid AS object_oid,
		se.flow_oid AS flow_oid,
		'E' AS `type`
	FROM etl_step se
	WHERE se.`type` = 'E'
	GROUP BY se.flow_oid, se.from_object_oid
)
UNION ALL
(
	SELECT 
		stl.to_object_oid AS object_oid,
		stl.flow_oid AS flow,
		stl.`type`
	FROM etl_step stl
	WHERE stl.`type` = 'T' OR stl.`type` = 'L'
	GROUP BY stl.flow_oid, stl.`type`, stl.to_object_oid
);