-- UP
IF COL_LENGTH('dbo.etl_step','step_flow_sequence') IS NULL
BEGIN
    DECLARE @addQuery NVARCHAR(MAX);
    SET @addQuery = 'ALTER TABLE [dbo].[etl_step] ADD [step_flow_sequence] INT NULL;'
    EXEC sp_executesql @addQuery;

    DECLARE @updateQuery NVARCHAR(MAX);
    SET @updateQuery =
        'UPDATE  s
        SET s.[step_flow_sequence] = srs.[flow_run_pos]
        FROM [dbo].[etl_step] s
        INNER JOIN (
          SELECT DISTINCT flow_run_pos, sr.flow_oid, sr.step_oid FROM etl_step_run sr
          LEFT JOIN etl_flow_run fr ON fr.oid = sr.flow_run_oid
          WHERE  fr.valid_flag = 1 AND fr.created_on = (Select MAX(created_on) FROM etl_flow_run WHERE flow_oid = fr.flow_oid)
        ) srs ON etl_step.oid = srs.step_oid AND etl_step.flow_oid = srs.flow_oid;'
    EXEC sp_executesql @updateQuery;

    DECLARE @alterQuery NVARCHAR(MAX);
    SET @alterQuery = 'ALTER TABLE [dbo].[etl_step] ALTER COLUMN [step_flow_sequence] INT NOT NULL;'
    EXEC sp_executesql @alterQuery;

    -- delete old value
    DECLARE @alterQuery2 NVARCHAR(MAX);
    SET @alterQuery2 = 'ALTER TABLE [dbo].[etl_step] DROP COLUMN [run_after_step_oid];'
    EXEC sp_executesql @alterQuery2;
END

-- DOWN
-- cannot change back structural changes