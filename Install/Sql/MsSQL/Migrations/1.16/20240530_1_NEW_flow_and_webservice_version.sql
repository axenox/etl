-- UP
IF COL_LENGTH('dbo.etl_webservice','version') IS NULL
    ALTER TABLE etl_webservice ADD version VARCHAR(10) NULL;

IF COL_LENGTH('dbo.etl_flow','version') IS NULL
    ALTER TABLE etl_flow ADD version VARCHAR(10) NULL;

-- DOWN
IF COL_LENGTH('dbo.etl_webservice','version') IS NOT NULL
    ALTER TABLE etl_webservice DROP version;

IF COL_LENGTH('dbo.etl_flow','version') IS NOT NULL
    ALTER TABLE etl_flow DROP version;