-- UP
IF COL_LENGTH('dbo.etl_webservice_flow', 'route') IS NOT NULL
    ALTER TABLE [dbo].[etl_webservice_flow] ALTER COLUMN [route] NVARCHAR(30) NULL;


IF COL_LENGTH('dbo.etl_webservice_flow', 'oid') IS NOT NULL
    ALTER TABLE [dbo].[etl_webservice_flow] ALTER COLUMN [oid] BINARY(16) NULL;