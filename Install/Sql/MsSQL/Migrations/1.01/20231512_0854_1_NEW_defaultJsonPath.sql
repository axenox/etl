-- UP
IF COL_LENGTH('dbo.etl_webservice_type','default_response_path') IS NULL
ALTER TABLE etl_webservice_type
ADD default_response_path nvarchar(300) NULL;

-- DOWN

IF COL_LENGTH('dbo.etl_webservice_type','default_response_path') IS NOT NULL
ALTER TABLE etl_webservice_type
DROP COLUMN default_response_path;
