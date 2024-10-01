-- UP
UPDATE dbo.etl_step SET etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetJsonToOpenApi.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetToOpenApi.php';
GO
UPDATE dbo.etl_step SET etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiJsonToDataSheet.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiToDataSheet.php';
GO

-- DOWN
UPDATE dbo.etl_step SET etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetToOpenApi.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetJsonToOpenApi.php';
GO
UPDATE dbo.etl_step SET etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiToDataSheet.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiJsonToDataSheet.php';
GO