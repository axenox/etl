-- UP

CREATE TABLE dbo.etl_file_flow (
  oid binary(16) NOT NULL,
  created_on datetime NOT NULL,
  modified_on datetime NOT NULL,
  created_by_user_oid binary(16) NOT NULL,
  modified_by_user_oid binary(16) NOT NULL,
  name nvarchar(100) NOT NULL,
  flow_oid binary(16) NOT NULL,
  alias nvarchar(100) NOT NULL,
  app_oid binary(16),
  description nvarchar(max),
  enabled_flag tinyint NOT NULL DEFAULT '1',
  PRIMARY KEY (oid)
);


CREATE TABLE dbo.etl_file_upload (
  oid binary(16) NOT NULL,
  created_on datetime NOT NULL,
  modified_on datetime NOT NULL,
  created_by_user_oid binary(16) NOT NULL,
  modified_by_user_oid binary(16) NOT NULL,
  file_flow_oid binary(16) NULL,
  file_name nvarchar(255) NOT NULL,
  file_mimetype nvarchar(100) NOT NULL,
  file_size_bytes int NOT NULL,
  file_md5 nvarchar(32),
  comment nvarchar(max),
  flow_run_oid binary(16),
  PRIMARY KEY (oid),
  CONSTRAINT FK_etl_file_upload_etl_file_flow_file_flow_oid FOREIGN KEY ([file_flow_oid]) REFERENCES [dbo].[etl_file_flow] ([oid]) ON DELETE SET NULL ON UPDATE NO ACTION
);
	
-- DOWN

-- Do not drop columns!