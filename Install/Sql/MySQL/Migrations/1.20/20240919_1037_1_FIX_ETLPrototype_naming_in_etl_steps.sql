-- UP
UPDATE `etl_step` SET etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetJsonToOpenApi.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetToOpenApi.php';
UPDATE `etl_step` SET etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiJsonToDataSheet.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiToDataSheet.php';

-- DOWN
UPDATE `etl_step` SET etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetToOpenApi.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/DataSheetJsonToOpenApi.php';
UPDATE `etl_step` SET etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiToDataSheet.php' WHERE etl_prototype_path = 'axenox/etl/ETLPrototypes/OpenApiJsonToDataSheet.php';