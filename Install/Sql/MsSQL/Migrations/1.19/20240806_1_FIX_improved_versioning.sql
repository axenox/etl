-- UP

-- Make version columns longer
IF COL_LENGTH('dbo.etl_webservice','version') IS NOT NULL
ALTER TABLE dbo.etl_webservice ALTER COLUMN version NVARCHAR(50) NULL;

IF COL_LENGTH('dbo.etl_flow','version') IS NOT NULL
ALTER TABLE dbo.etl_flow ALTER COLUMN version NVARCHAR(50) NULL;

-- Remove request_direction from web services
IF COL_LENGTH('dbo.etl_webservice','request_direction') IS NOT NULL
BEGIN
	DECLARE @sql NVARCHAR(MAX),
			@schema NVARCHAR(50) = 'dbo',
			@table NVARCHAR(50) = 'etl_webservice',
			@column NVARCHAR(50) = 'request_direction'
	/* DROP default constraints	*/
	WHILE 1=1
	BEGIN
		SELECT TOP 1 @sql = N'ALTER TABLE '+@schema+'.'+@table+' DROP CONSTRAINT ['+dc.NAME+N']'
			FROM sys.default_constraints dc
				JOIN sys.columns c ON c.default_object_id = dc.object_id
			WHERE 
				dc.parent_object_id = OBJECT_ID(@table)
				AND c.name = @column
		IF @@ROWCOUNT = 0 BREAK
		EXEC (@sql)
	END
	/* DROP foreign keys */
	WHILE 1=1
	BEGIN
		SELECT TOP 1 @sql = N'ALTER TABLE '+@schema+'.'+@table+' DROP CONSTRAINT ['+fk.NAME+N']'
			FROM sys.foreign_keys fk
				JOIN sys.foreign_key_columns fk_cols ON fk_cols.constraint_object_id = fk.object_id
			WHERE 
				fk.parent_object_id = OBJECT_ID(@table)
				AND COL_NAME(fk.parent_object_id, fk_cols.parent_column_id) = @column
		IF @@ROWCOUNT = 0 BREAK
		EXEC (@sql)
	END
	/* DROP column */
	EXEC(N'ALTER TABLE ['+@schema+'].['+@table+'] DROP COLUMN ['+@column+']')
END

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