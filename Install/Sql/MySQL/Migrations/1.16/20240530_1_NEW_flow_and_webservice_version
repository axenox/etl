-- UP
CALL execute_sql_on_missing_column('etl_webservice', 'version', 'ALTER TABLE etl_webservice ADD version VARCHAR(10) NULL ');
CALL execute_sql_on_missing_column('etl_flow', 'version', 'ALTER TABLE etl_flow ADD version VARCHAR(10) NULL ');

-- DOWN
CALL execute_sql_on_existing_column('etl_webservice', 'version', 'ALTER TABLE etl_webservice DROP version');
CALL execute_sql_on_existing_column('etl_flow', 'version', 'ALTER TABLE etl_flow DROP version');