-- UP
IF OBJECT_ID ('dbo.etl_webservice_flow', N'U') IS NULL
CREATE TABLE etl_webservice_flow(
    [oid] VARBINARY(16) NOT NULL,
    [created_on] DATETIME NOT NULL,
    [modified_on] DATETIME NOT NULL,
    [created_by_user_oid] BINARY(16) NOT NULL,
    [modified_by_user_oid] BINARY(16) NOT NULL,
    [webservice_oid] BINARY(16) NOT NULL,
    [flow_oid] BINARY(16) NOT NULL,
    [route] BINARY(30),
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
ALTER TABLE etl_webservice
    DROP flow_oid;

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

