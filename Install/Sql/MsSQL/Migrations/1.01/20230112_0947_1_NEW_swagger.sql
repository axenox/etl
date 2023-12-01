-- UP

ALTER TABLE dbo.etl_webservice_route
ADD swagger_json nvarchar(max) NULL;

-- DOWN

ALTER TABLE dbo.etl_webservice_route
DROP swagger_json;
