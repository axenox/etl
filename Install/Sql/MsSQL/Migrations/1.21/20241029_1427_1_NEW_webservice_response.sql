-- UP
-- create new response table
CREATE TABLE [dbo].[etl_webservice_response](
    [oid] [binary](16) NOT NULL,
    [created_on] [datetime2](7) NOT NULL,
    [modified_on] [datetime2](7) NOT NULL,
    [created_by_user_oid] [binary](16) NOT NULL,
    [modified_by_user_oid] [binary](16) NOT NULL,
    [webservice_request_oid] [binary](16) NULL,
    [http_response_code] [smallint] NOT NULL,
    [http_content_type] [nvarchar](200) NULL,
    [status] [tinyint] NOT NULL DEFAULT 10,
    [result_text] [nvarchar](max) NULL,
    [error_logid] [nvarchar](20) NULL,
    [error_message] [nvarchar](max) NULL,
    [response_body] [nvarchar](max) NULL,
    [response_header] [nvarchar](max) NULL,
    PRIMARY KEY CLUSTERED ([oid] ASC)
    )
ALTER TABLE [dbo].[etl_webservice_response]
    ADD CONSTRAINT [fk_webservice_response_request] FOREIGN KEY ([webservice_request_oid])
    REFERENCES [dbo].[etl_webservice_request]([oid])
    GO
-- move data from request to response table
    INSERT INTO [dbo].[etl_webservice_response](
    oid, created_on, modified_on, created_by_user_oid, modified_by_user_oid, webservice_request_oid, http_response_code, http_content_type,
    status, result_text, error_logid, error_message, response_body, response_header
    )
select r.oid,r.created_on,r.modified_on,r.created_by_user_oid,r.modified_by_user_oid, r.oid [webservice_request_oid],
       r.http_response_code, IIF(ISJSON(r.response_header)=1, JSON_VALUE(r.response_header,'$."Content-Type"[0]'), NULL) [http_content_type],
       r.status, r.result_text,
       r.error_logid, r.error_message,
       r.response_body, r.response_header
from dbo.etl_webservice_request r
where r.http_response_code IS NOT NULL
  and not exists (select 1 from [dbo].[etl_webservice_response] e where e.oid = r.oid);
-- add new column to table
ALTER TABLE [dbo].[etl_webservice_request] ADD [http_querystring] [nvarchar](max) NULL;
-- find Constraints from columns to be dropped
DECLARE @sql NVARCHAR(MAX)
WHILE 1=1
BEGIN
SELECT TOP 1 @sql = N'ALTER TABLE [dbo].[etl_webservice_request] DROP CONSTRAINT ['+dc.NAME+N']'
FROM sys.default_constraints dc
         INNER JOIN sys.columns c
                    ON c.default_object_id = dc.object_id
WHERE
    dc.parent_object_id = OBJECT_ID('[dbo].[etl_webservice_request]')
  AND c.name IN (N'http_response_code',N'result_text',N'response_body',N'response_header')
    IF @@ROWCOUNT = 0 BREAK
    EXEC (@sql)
END
-- drop request columns
ALTER TABLE [dbo].[etl_webservice_request] DROP COLUMN [http_response_code];
ALTER TABLE [dbo].[etl_webservice_request] DROP COLUMN [result_text];
ALTER TABLE [dbo].[etl_webservice_request] DROP COLUMN [response_body];
ALTER TABLE [dbo].[etl_webservice_request] DROP COLUMN [response_header];
GO
-- DOWN
-- recreate columns
ALTER TABLE [dbo].[etl_webservice_request] ADD [http_response_code] SMALLINT NULL DEFAULT 10;
ALTER TABLE [dbo].[etl_webservice_request] ADD [result_text] NVARCHAR(MAX) NULL;
ALTER TABLE [dbo].[etl_webservice_request] ADD [response_body] NVARCHAR(MAX) NULL;
ALTER TABLE [dbo].[etl_webservice_request] ADD [response_header] NVARCHAR(MAX) NULL;
GO
-- drop new column from table
ALTER TABLE [dbo].[etl_webservice_request] DROP COLUMN [http_querystring];
-- move data from response to request table
MERGE INTO [dbo].[etl_webservice_request] trg
    USING (
    SELECT r.webservice_request_oid, r.http_response_code, r.result_text, r.response_body, r.response_header, r.http_content_type
    FROM [dbo].[etl_webservice_response] r
    ) src
    ON (src.webservice_request_oid = trg.oid)
    WHEN MATCHED THEN UPDATE SET trg.http_response_code = src.http_response_code, trg.result_text = src.result_text,
                          trg.response_body = src.response_body, trg.response_header = src.response_header, trg.http_content_type = src.http_content_type;
GO
-- drop new table for responses
DROP TABLE [dbo].[etl_webservice_response];
GO
