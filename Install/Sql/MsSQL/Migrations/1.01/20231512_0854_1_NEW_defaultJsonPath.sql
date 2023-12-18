-- UP
ALTER TABLE 'etl_webservice_type'
ADD 'default_response_path' nvarchar(300) NULL;

-- DOWN

ALTER TABLE 'etl_webservice_type'
DROP COLUMN 'default_response_path';
