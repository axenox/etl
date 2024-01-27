-- UP

-- Service types
IF OBJECT_ID ('etl_webservice_type', N'U') IS NULL 
CREATE TABLE etl_webservice_type (
  oid binary(16) NOT NULL,
  created_on datetime NOT NULL,
  modified_on datetime NOT NULL,
  created_by_user_oid binary(16) NOT NULL,
  modified_by_user_oid binary(16) NOT NULL,
  name nvarchar(100) NOT NULL,
  version nvarchar(50),
  app_oid binary(16) DEFAULT NULL,
  schema_json nvarchar(max),
  PRIMARY KEY (oid)
);

IF COL_LENGTH('dbo.etl_webservice_route', 'type_oid') IS NULL
ALTER TABLE dbo.etl_webservice_route
	ADD type_oid binary(16) NULL;

-- store validation response
IF COL_LENGTH('dbo.etl_webservice_request', 'response_body') IS NULL
ALTER TABLE dbo.etl_webservice_request
	ADD response_body nvarchar(max) NULL;
IF COL_LENGTH('dbo.etl_webservice_request', 'response_header') IS NULL
ALTER TABLE etl_webservice_request
	ADD response_header nvarchar(max) NULL;

-- DOWN

IF COL_LENGTH('dbo.etl_webservice_request', 'response_body') IS NOT NULL
ALTER TABLE etl_webservice_request
	DROP COLUMN response_body;
IF COL_LENGTH('dbo.etl_webservice_request', 'response_header') IS NOT NULL
ALTER TABLE etl_webservice_request
	DROP COLUMN response_header;

IF COL_LENGTH('dbo.etl_webservice_route','type_oid') IS NOT NULL
ALTER TABLE dbo.etl_webservice_route
DROP COLUMN type_oid;

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
	EXEC('DROP TABLE ' + @schema + '.' + @table);
END