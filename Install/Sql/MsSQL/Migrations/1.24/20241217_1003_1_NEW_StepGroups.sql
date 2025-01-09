-- UP

ALTER TABLE dbo.etl_step
ADD parent_step_oid binary(16) NULL;

ALTER TABLE dbo.etl_step
ALTER COLUMN to_object_oid binary(16) NULL;

-- DOWN
