-- UP
IF OBJECT_ID ('etl_webservice_type', N'U') IS NULL 
CREATE TABLE 'etl_webservice_type' (
CREATE TABLE IF NOT EXISTS  (
  'oid' binary(16) NOT NULL,
  'created_on' datetime NOT NULL,
  'modified_on' datetime NOT NULL,
  'created_by_user_oid' binary(16) NOT NULL,
  'modified_by_user_oid' binary(16) NOT NULL,
  'name' varchar(100) NOT NULL,
  'version' varchar(50),
  'app_oid' binary(16) DEFAULT NULL,
  'schema_json' nvarchar(max),
  PRIMARY KEY ('oid')

ALTER TABLE 'etl_webservice_route'
ADD 'type_oid' binary(16) NULL;

-- for validation of schema:
ALTER TABLE 'etl_webservice_request'
ADD 'response_body' nvarchar(max) NULL,
ADD 'response_header' varchar(500) NULL;

-- DOWN
DECLARE @table NVARCHAR(max) = 'etl_webservice_type';
DECLARE @schema NVARCHAR(max) = 'dbo';
DECLARE @stmt NVARCHAR(max)

IF OBJECT_ID (CONCAT(@schema, '.', @table), N'U') IS NOT NULL
BEGIN	
	-- remove constraints inside this table
	SELECT @stmt = '';
	SELECT @stmt += N'
	ALTER TABLE ' + OBJECT_NAME(parent_object_id) + ' DROP CONSTRAINT ' + OBJECT_NAME(object_id) + ';' 
	FROM SYS.OBJECTS
	WHERE TYPE_DESC LIKE '%CONSTRAINT' AND OBJECT_NAME(parent_object_id) = @table AND SCHEMA_NAME(schema_id) = @schema;
	EXEC(@stmt);

	-- drop the table itself
	DROP TABLE CONCAT(@schema, '.', @table);
END

ALTER TABLE CONCAT(@schema, '.', 'etl_webservice_route');
DROP COLUMN 'type_oid';