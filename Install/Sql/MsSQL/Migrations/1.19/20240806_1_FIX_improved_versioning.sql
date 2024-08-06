-- UP

-- Make version columns longer
IF COL_LENGTH('dbo.etl_webservice','version') IS NOT NULL
ALTER TABLE dbo.etl_webservice ALTER COLUMN version NVARCHAR(50) NULL;

IF COL_LENGTH('dbo.etl_flow','version') IS NOT NULL
ALTER TABLE dbo.etl_flow ALTER COLUMN version NVARCHAR(50) NULL;

-- Remove request_direction from web services
IF COL_LENGTH('dbo.etl_webservice','request_direction') IS NOT NULL
ALTER TABLE dbo.etl_webservice DROP COLUMN request_direction;

-- Make local url required
IF COL_LENGTH('dbo.etl_webservice','local_url') IS NOT NULL
ALTER TABLE dbo.etl_webservice ALTER COLUMN local_url NVARCHAR(400) NOT NULL;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE NAME = N'U_etl_webservice_path_version')
CREATE INDEX [U_etl_webservice_path_version] ON [dbo].[etl_webservice] ([local_url], [version]);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE NAME = N'U_etl_flow_alias_version')
CREATE INDEX [U_etl_flow_alias_version] ON [dbo].[etl_flow] ([alias], [version]);
	
-- DOWN

IF EXISTS (SELECT * FROM sys.indexes WHERE NAME = N'U_etl_webservice_path_version') 
DROP INDEX [U_etl_webservice_path_version] ON [dbo].[etl_webservice];

IF EXISTS (SELECT * FROM sys.indexes WHERE NAME = N'U_etl_flow_alias_version') 
DROP INDEX [U_etl_flow_alias_version] ON [dbo].[etl_flow];

IF COL_LENGTH('dbo.etl_webservice','version') IS NOT NULL
ALTER TABLE dbo.etl_webservice ALTER COLUMN version NVARCHAR(10) NULL;

IF COL_LENGTH('dbo.etl_flow','version') IS NOT NULL
ALTER TABLE dbo.etl_flow ALTER COLUMN version NVARCHAR(10) NULL;

IF COL_LENGTH('dbo.etl_webservice','local_url') IS NOT NULL
ALTER TABLE dbo.etl_webservice ALTER COLUMN local_url NVARCHAR(400) NULL;

IF COL_LENGTH('dbo.etl_webservice','request_direction') IS NULL
ALTER TABLE dbo.etl_webservice ADD request_direction NVARCHAR(10) NOT NULL DEFAULT 'Inbound';