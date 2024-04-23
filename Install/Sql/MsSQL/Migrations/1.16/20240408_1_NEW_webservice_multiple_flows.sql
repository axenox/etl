-- UP
IF OBJECT_ID ('dbo.etl_webservice_flow', N'U') IS NULL
CREATE TABLE etl_webservice_flow(
    [oid] BINARY(16) NOT NULL,
    [created_on] DATETIME NOT NULL,
    [modified_on] DATETIME NOT NULL,
    [created_by_user_oid] BINARY(16) NOT NULL,
    [modified_by_user_oid] BINARY(16) NOT NULL,
    [webservice_oid] BINARY(16) NOT NULL,
    [flow_oid] BINARY(16) NOT NULL,
    [route] VARCHAR(30),
    CONSTRAINT [FK_route_to_flow] FOREIGN KEY ([flow_oid]) REFERENCES [etl_flow] ([oid]),
    CONSTRAINT [FK_flow_to_route] FOREIGN KEY ([webservice_oid]) REFERENCES [etl_webservice] ([oid])
);

INSERT INTO [etl_webservice_flow]
SELECT CONVERT(VARBINARY(16), REPLACE(NEWID(), '-', '')),
       GETDATE(),
       GETDATE(),
       created_by_user_oid,
       created_by_user_oid,
       oid,
       flow_oid,
       NULL
FROM [etl_webservice] w
WHERE [flow_oid] IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM etl_webservice_flow wf
    WHERE wf.webservice_oid = w.oid
);

IF COL_LENGTH('dbo.etl_webservice','flow_oid') IS NOT NULL
BEGIN
	DECLARE @sql NVARCHAR(MAX),
			@schema NVARCHAR(50) = 'dbo',
			@table NVARCHAR(50) = 'etl_webservice',
			@column NVARCHAR(50) = 'flow_oid'
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
END;

-- DOWN
IF COL_LENGTH('dbo.etl_webservice','flow_oid') IS NULL
ALTER TABLE etl_webservice
    ADD flow_oid BINARY(16) NULL;

UPDATE w
SET w.[flow_oid] = wf.[flow_oid]
FROM [etl_webservice] w
INNER JOIN [etl_webservice_flow] wf ON wf.[webservice_oid] = w.[oid];

IF OBJECT_ID ('dbo.etl_webservice_flow', N'U') IS NOT NULL
DROP TABLE [etl_webservice_flow];

