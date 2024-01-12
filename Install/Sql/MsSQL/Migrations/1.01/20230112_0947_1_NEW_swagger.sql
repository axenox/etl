-- UP
IF COL_LENGTH('dbo.etl_webservice_route','swagger_json') IS NULL
ALTER TABLE dbo.etl_webservice_route
ADD swagger_json nvarchar(max) NULL;

-- DOWN

IF COL_LENGTH('dbo.etl_webservice_route','swagger_json') IS NOT NULL
ALTER TABLE dbo.etl_webservice_route
DROP COLUMN swagger_json;
