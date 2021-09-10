IF OBJECT_ID('dbo.exf_monitor_error', 'U') IS NULL 
CREATE TABLE [dbo].[exf_monitor_error] (
  	[oid] [binary](16) NOT NULL,
	[created_on] [datetime2] NOT NULL,
	[modified_on] [datetime2] NOT NULL,
	[created_by_user_oid] [binary](16),
	[modified_by_user_oid] [binary](16),
    [log_id] nvarchar(10) NOT NULL,
    [error_level] nvarchar(20) NOT NULL,
    [error_widget] nvarchar(max) NOT NULL,
    [message] nvarchar(max) NOT NULL,
    [date] date NOT NULL,
    [status] smallint NOT NULL,
    [user_oid] binary(16),
    [action_oid] binary(16),
  CONSTRAINT [PK_exf_monitor_error_oid] PRIMARY KEY CLUSTERED (oid)
);

IF OBJECT_ID('dbo.etl_flow', 'U') IS NULL 
CREATE TABLE [dbo].[etl_flow] (
  [oid] binary(16) NOT NULL,
  [created_on] datetime2 NOT NULL,
  [modified_on] datetime2 NOT NULL,
  [created_by_user_oid] binary(16) NOT NULL,
  [modified_by_user_oid] binary(16) NOT NULL,
  [name] nvarchar(50) NOT NULL,
  [alias] nvarchar(50) NOT NULL,
  [description] nvarchar(max),
  [app_oid] binary(16),
  [scheduler_oid] binary(16),
  CONSTRAINT [PK_etl_flow] PRIMARY KEY CLUSTERED (oid)
);

IF OBJECT_ID('dbo.etl_step', 'U') IS NULL 
CREATE TABLE [dbo].[etl_step] (
  [oid] binary(16) NOT NULL,
  [created_on] datetime2 NOT NULL,
  [modified_on] datetime2 NOT NULL,
  [created_by_user_oid] binary(16) NOT NULL,
  [modified_by_user_oid] binary(16) NOT NULL,
  [name] nvarchar(50) NOT NULL,
  [type] char(1) NOT NULL,
  [description] nvarchar(max),
  [flow_oid] binary(16) NOT NULL,
  [from_object_oid] binary(16),
  [to_object_oid] binary(16) NOT NULL,
  [etl_prototype_path] nvarchar(250) NOT NULL,
  [etl_config_uxon] nvarchar(max),
  [disabled] tinyint NOT NULL DEFAULT 0,
  [stop_flow_on_error] tinyint NOT NULL DEFAULT '1',
  [run_after_step_oid] binary(16),
  CONSTRAINT [PK_etl_step] PRIMARY KEY CLUSTERED (oid)
);


IF OBJECT_ID('dbo.etl_step_run', 'U') IS NULL 
CREATE TABLE [dbo].[etl_step_run] (
  [oid] binary(16) NOT NULL,
  [created_on] datetime2 NOT NULL,
  [modified_on] datetime2 NOT NULL,
  [created_by_user_oid] binary(16) NOT NULL,
  [modified_by_user_oid] binary(16) NOT NULL,
  [step_oid] binary(16) NOT NULL,
  [flow_oid] binary(16) NOT NULL,
  [flow_run_oid] binary(16) NOT NULL,
  [flow_run_pos] int NOT NULL,
  [start_time] datetime2 NOT NULL,
  [debug_widget] nvarchar(max),
  [timeout_seconds] int NOT NULL,
  [end_time] datetime2,
  [result_count] int NOT NULL DEFAULT 0,
  [result_uxon] nvarchar(max),
  [output] ntext,
  [incremental_flag] tinyint NOT NULL DEFAULT 0,
  [incremental_after_run_oid] binary(16),
  [step_disabled_flag] tinyint NOT NULL DEFAULT 0,
  [success_flag] tinyint NOT NULL DEFAULT 0,
  [skipped_flag] tinyint NOT NULL DEFAULT 0,
  [invalidated_flag] tinyint NOT NULL DEFAULT 0,
  [error_flag] tinyint NOT NULL DEFAULT 0,
  [error_message] nvarchar(200),
  [error_log_id] nvarchar(10),
  [error_widget] ntext,
  CONSTRAINT [PK_etl_step_run] PRIMARY KEY CLUSTERED (oid),
  CONSTRAINT [UC_etl_step_run_pos_unique_per_flow_run] UNIQUE ([flow_run_oid],[flow_run_pos])
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE NAME = N'IDX_etl_flow_run') 
CREATE INDEX [IDX_etl_flow_run] ON [dbo].[etl_step_run] ([flow_run_oid],[flow_oid],[start_time],[end_time]);