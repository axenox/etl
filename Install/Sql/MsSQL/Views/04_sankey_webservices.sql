IF OBJECT_ID('dbo.etl_sankey_webservices', 'V') IS NOT NULL
    DROP VIEW dbo.etl_sankey_webservices;
GO

IF OBJECT_ID('dbo.etl_flow_sankey', 'V') IS NOT NULL
    DROP VIEW dbo.etl_flow_sankey;
GO

CREATE VIEW dbo.etl_flow_sankey AS
SELECT
	source_level,
	target_level,
	source_object_oid,
	target_object_oid,
	name,
	step_oid,
	flow_oid
FROM
(	
	(
		SELECT 
		    fof.level AS source_level,
		    fot.level AS target_level,
		    s.from_object_oid AS source_object_oid,
		    s.to_object_oid AS target_object_oid,
		    s.name,
		    s.oid AS step_oid,
		    s.flow_oid
		FROM etl_step s
		    INNER JOIN etl_flow_objects fof ON s.flow_oid = fof.flow_oid 
		    	AND fof.object_oid = s.from_object_oid
		    INNER JOIN etl_flow_objects fot ON s.flow_oid = fot.flow_oid 
		    	AND fot.object_oid = s.to_object_oid
    )
    UNION ALL 
    (		    	
    	SELECT 
		    fot.level AS source_level,
		    fot.level + 1 AS target_level,
		    s.to_object_oid AS source_object_oid,
		    0x11eeae4695964d78ae46025041000001 AS target_object_oid, -- UID of the object axenox.ETL.webservice_request
		    'Send HTTP Response' AS name,
		    NULL AS step_oid,
		    fot.flow_oid AS flow_oid
		FROM
		    etl_step s
		    INNER JOIN etl_webservice_flow wf ON wf.flow_oid = s.flow_oid
            INNER JOIN etl_webservice ws ON ws.oid = wf.webservice_oid
		    	AND ws.flow_direction = 'OUT'
		    INNER JOIN etl_flow_objects fot ON s.flow_oid = fot.flow_oid 
		    	AND fot.object_oid = s.to_object_oid
		WHERE 
		    fot.level = (SELECT MAX(fo1.level) FROM etl_flow_objects fo1 WHERE fot.flow_oid = fo1.flow_oid) 		    
			    
    )
    UNION ALL 
    (		    	
    	SELECT 
		    fof.level - 1 AS source_level,
		    fof.level AS target_level,
		    0x11eeae4695964d78ae46025041000001 AS source_object_oid, -- UID of the object axenox.ETL.webservice_request
		    s.from_object_oid AS target_object_oid,
		    'Recieve HTTP Request' AS name,
		    NULL AS step_oid,
		    fof.flow_oid AS flow_oid
		FROM
		    etl_step s
            INNER JOIN etl_webservice_flow wf ON wf.flow_oid = s.flow_oid
            INNER JOIN etl_webservice ws ON ws.oid = wf.webservice_oid
		    	AND ws.flow_direction = 'IN'
		    INNER JOIN etl_flow_objects fof ON s.flow_oid = fof.flow_oid 
		    	AND fof.object_oid = s.from_object_oid
		WHERE 
		    fof.level = (SELECT MIN(fo1.level) FROM etl_flow_objects fo1 WHERE fof.flow_oid = fo1.flow_oid) 
	    
    )
) sankeydata