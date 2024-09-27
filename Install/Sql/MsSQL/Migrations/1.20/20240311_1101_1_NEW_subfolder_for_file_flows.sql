-- UP

ALTER TABLE dbo.etl_file_flow
ADD subfolder nvarchar(255) NULL;

ALTER TABLE dbo.etl_file_upload
ADD CONSTRAINT FK_dbo_file_upload_file_flow_oid FOREIGN KEY (file_flow_oid) REFERENCES dbo.etl_file_flow (oid);
	
-- DOWN

ALTER TABLE dbo.etl_file_flow
DROP COLUMN subfolder;

ALTER TABLE dbo.etl_file_upload
DROP CONSTRAINT FK_dbo_file_upload_file_flow_oid;