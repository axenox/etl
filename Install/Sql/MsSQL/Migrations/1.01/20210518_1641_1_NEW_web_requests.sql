-- UP

-- exface.etl_webservice_route
IF OBJECT_ID ('dbo.etl_webservice_route', N'U') IS NULL 
CREATE TABLE dbo.etl_webservice_route (
  oid binary(16) NOT NULL,
  created_on datetime2 NOT NULL,
  modified_on datetime2 NOT NULL,
  created_by_user_oid binary(16) NOT NULL,
  modified_by_user_oid binary(16) NOT NULL,
  name nvarchar(100) NOT NULL,
  alias nvarchar(100) NOT NULL,
  app_oid binary(16) DEFAULT NULL,
  description nvarchar(max),
  direction nvarchar(3) NOT NULL DEFAULT 'IN',
  flow_oid binary(16) NOT NULL FOREIGN KEY REFERENCES dbo.etl_flow (oid),
  in_url nvarchar(400) DEFAULT NULL,
  out_connection_oid binary(16) DEFAULT NULL,
  config_uxon nvarchar(max),
  PRIMARY KEY (oid)
);

-- exface.etl_webservice_request
IF OBJECT_ID ('dbo.etl_webservice_request', N'U') IS NULL 
CREATE TABLE dbo.etl_webservice_request (
  oid binary(16) NOT NULL,
  created_on datetime2 NOT NULL,
  modified_on datetime2 NOT NULL,
  created_by_user_oid binary(16) NOT NULL,
  modified_by_user_oid binary(16) NOT NULL,
  route_oid binary(16) DEFAULT NULL FOREIGN KEY REFERENCES dbo.etl_webservice_route (oid),
  flow_run_uid binary(16) DEFAULT NULL,
  url nvarchar(max) NOT NULL,
  url_path nvarchar(max) NOT NULL,
  http_method nvarchar(10) NOT NULL,
  http_response_code smallint DEFAULT NULL,
  http_headers nvarchar(max) NOT NULL,
  http_body nvarchar(max),
  http_content_type nvarchar(200) DEFAULT NULL,
  status tinyint NOT NULL DEFAULT '10',
  result_text nvarchar(max),
  error_message nvarchar(max),
  error_logid nvarchar(20) DEFAULT NULL,
  PRIMARY KEY (oid)
);
	
-- DOWN

/* Do not drop anything to keep historical data */