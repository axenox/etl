<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractOpenApiPrototype;
use exface\Core\CommonLogic\Filesystem\DataSourceFileInfo;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\DataTypes\ArrayDataType;
use exface\Core\DataTypes\BinaryDataType;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\DataTypes\ComparatorDataType;
use exface\Core\DataTypes\DateDataType;
use exface\Core\DataTypes\DateTimeDataType;
use exface\Core\DataTypes\IntegerDataType;
use exface\Core\DataTypes\NumberDataType;
use exface\Core\DataTypes\StringEnumDataType;
use exface\Core\DataTypes\TimeDataType;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\Factories\DataTypeFactory;
use exface\Core\Factories\FormulaFactory;
use exface\Core\Factories\DataSheetFactory;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Factories\MetaObjectFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\DataTypes\EnumDataTypeInterface;
use exface\Core\Interfaces\Model\Behaviors\FileBehaviorInterface;
use exface\Core\QueryBuilders\ExcelBuilder;
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use Flow\JSONPath\JSONPath;
use Flow\JSONPath\JSONPathException;
use axenox\ETL\Common\UxonEtlStepResult;

/**
 * Objects have to be defined with an x-object-alias and with x-attribute-aliases for the object to fill
 * AND x-excel-sheet and with x-excel-column for the information where to read the information in the excel
 * like:
 * ´´´
 * {
 *     "Activities": {
 *          "type": "object",
 *          "x-object-alias": "full.namespace.object",
 *          "x-excel-sheet": "Activities",
 *          "properties: {
 *              "Activity_Id" {
 *                  "type": "string",
 *                  "x-attribute-alias": "attribute_alias",
 *                  "x-excel-column": "Activity_Id"
 *              }
 *          }
 *     }
 * }
 *
 * ´´´
 *
 *
 * Placeholder and STATIC Formulas can be defined wihtin the configuration.
 * "additional_rows": [
 *      {
 *          "attribute_alias": "ETLFlowRunUID",
 *          "value": "[#flow_run_uid#]"
 *      },
 *      {
 *          "attribute_alias": "RequestId",
 *          "value": "=Lookup('UID', 'axenox.ETL.webservice_request', 'flow_run = [#flow_run_uid#]')"
 *      },
 *      {
 *          "attribute_alias": "Betreiber",
 *          "value": "SuedLink"
 *      }
 * ]
 *
 * @author miriam.seitz
 */
class OpenApiExcelToDataSheet extends AbstractOpenApiPrototype
{
    const OPEN_API_ATTRIBUTE_TO_EXCEL_SHEET = 'x-excel-sheet';
    const OPEN_API_ATTRIBUTE_TO_EXCEL_COLUMN = 'x-excel-column';

    private $additionalColumns = null;
    private $schemaName = null;
    private array $webservice;
    private $filepath;

    /**
     *
     * {@inheritDoc}
     * @throws JSONPathException|\Throwable
     * @see \axenox\ETL\Interfaces\ETLStepInterface::run()
     */
    public function run(ETLStepDataInterface $stepData) : \Generator
    {
        $stepRunUid = $stepData->getStepRunUid();
        $placeholders = $this->getPlaceholders($stepData);
        $baseSheet = DataSheetFactory::createFromObject($this->getToObject());
        $result = new UxonEtlStepResult($stepRunUid);
        $task = $stepData->getTask();

        // If the task has input data and that data is a file, use it here.
        // Otherwise look for the flow run UID in the data of axenox.ETL.file_upload object.
        $fileData = $task->getInputData();
        if (
            $fileData !== null
            && ! $fileData->isEmpty()
            && $fileData->hasUidColumn(true)
            && ! $fileData->getMetaObject()->getBehaviors()->getByPrototypeClass(FileBehaviorInterface::class)->isEmpty()
        ) {
            $fileObj = $task->getMetaObject();
        } else {
            $fileObj = MetaObjectFactory::createFromString($this->getWorkbench(), 'axenox.ETL.file_upload');
            $fileData = DataSheetFactory::createFromObject($fileObj);
            $fileData->getFilters()->addConditionFromString('flow_run', $stepData->getFlowRunUid(), ComparatorDataType::EQUALS);
            $fileData->getColumns()->addFromUidAttribute();
            $fileData->dataRead();
        }
        $uploadUid = $fileData->getUidColumn()->getValue(0);
        $placeholders['upload_uid'] = $uploadUid;

        // If there is no file to read, stop here.
        // TODO Or throw an error? Need a step config property here!
        if ($uploadUid === null) {
            yield 'No file found in step input' . PHP_EOL;
            return $result->setProcessedRowsCounter(0);
        }

        // Create a FileInfo object for the Excel file
        $fileInfo = DataSourceFileInfo::fromObjectAndUID($fileObj, $uploadUid);
        yield 'Processing file "' . $fileInfo->getFilename() . '"' . PHP_EOL;

        // Reads the OpenAPI specification from the configrued webservice and transforms it into an excel column mapping
        $webservice = $this->getWebservice();
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice');
        $ds->getColumns()->addMultiple(
            ['UID', 'type__schema_json', 'swagger_json', 'enabled']);
        $ds->getFilters()->addConditionFromString('alias', $webservice['alias']);
        $ds->getFilters()->addConditionFromString('version', $webservice['version']);
        $ds->dataRead();

        $webservice = $ds->getSingleRow();
        $openApiJson = $webservice['swagger_json'];
        $schemas = (new JSONPath(json_decode($openApiJson, false)))
            ->find(self::JSON_PATH_TO_OPEN_API_SCHEMAS)->getData()[0];
        $schemas = json_decode(json_encode($schemas), true);
        if (($schemaName = $this->getSchemaName()) !== null && array_key_exists($schemaName, $schemas)) {
            $toObjectSchema = $schemas[$schemaName];
        } else {
            $toObjectSchema = $this->findObjectSchema($baseSheet, $schemas);
        }

        $excelColumnMapping = [];

        $sheetname = $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_EXCEL_SHEET];
        foreach ($toObjectSchema['properties'] as $propertyValue) {
            $excelColumnMapping[$propertyValue[self::OPEN_API_ATTRIBUTE_TO_EXCEL_COLUMN]] = [
                "attribute-alias" => $propertyValue[self::OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_ALIAS],
                "datatype" => ArrayDataType::getValueIfKeyExists($propertyValue, self::OPEN_API_ATTRIBUTE_TO_DATATYPE),
                "format" => ArrayDataType::getValueIfKeyExists($propertyValue, self::OPEN_API_ATTRIBUTE_TO_FORMAT),
                "enum-values" => ArrayDataType::getValueIfKeyExists($propertyValue, self::OPEN_API_ATTRIBUTE_TO_ENUM_VALUES)
            ];
        }

        // Create fake meta object with the expected attributes and use the regular
        // ExcelBuilder to read it.
        $fakeObj = MetaObjectFactory::createTemporary(
            $this->getWorkbench(),
            'Temp. Excel',
            $fileInfo->getPathAbsolute() . '/*[' . $sheetname . ']',
            ExcelBuilder::class,
            'exface.Core.objects_with_filebehavior'
        );
        foreach ($excelColumnMapping as $excelDataAddress => $propertyInfomation) {
            $dataType = $this->getInternalDatatype($propertyInfomation['datatype'], $propertyInfomation['format'], $propertyInfomation['enum-values']);
            MetaObjectFactory::addAttributeTemporary($fakeObj, $propertyInfomation['attribute-alias'], $propertyInfomation['attribute-alias'], '[' .$excelDataAddress . ']', $dataType);
        }
        $fakeSheet = DataSheetFactory::createFromObject($fakeObj);
        $fakeSheet->getColumns()->addFromAttributeGroup($fakeObj->getAttributes());
        $fakeSheet->dataRead();

        // Transform into Datasheet to Import
        $dsToImport = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS]);
        $dsToImport->getColumns()->addFromSystemAttributes();

        // transforms and validates excel data
        $importData = $this->getRowsToImport($fakeSheet, $placeholders);
        yield 'Importing rows ' . count($importData) . ' for ' . $dsToImport->getMetaObject()->getAlias(). ' with the data from a file import.';

        $transaction = $this->getWorkbench()->data()->startTransaction();
        $dsToImport->addRows($importData);
        try {
            $dsToImport->dataUpdate(true, $transaction);
        } catch (\Throwable $e) {
            $transaction->rollback();
            throw $e;
        }
        $transaction->commit();

        return $result->setProcessedRowsCounter($dsToImport->countRows());
    }

    /**
     * Add necessary import data to result sheet.
     *
     * @param mixed $requestBody
     * @param array $placeholder
     * @return array
     */
    protected function getRowsToImport(DataSheetInterface $excelResult, array $placeholder) : array
    {
        $rowsToImport = [];
        foreach ($excelResult->getRows() as $row) {
            // validate row values with OpenApi definition present in meta object
            foreach ($excelResult->getMetaObject()->getAttributes() as $attribute) {
                $dataType = $attribute->getDataType();
                $dataType->parse($row[$attribute->getAlias()]);
            }

            $rowsToImport[] = $this->addAdditionalColumnsToRow($placeholder, $row);
        }

        return $rowsToImport;
    }

    /**
     * Returns row with additional data provided by the ´additional_rows´ config within the step to every row.
     *
     * @param array $placeholder
     * @param array $row
     * @return array
     */
    public function addAdditionalColumnsToRow(array $placeholder, array $row) : array
    {
        $additionalColumn = $this->getAdditionalColumn();
        foreach ($additionalColumn as $column) {
            $value = $column['value'];
            switch (true) {
                case str_contains($value, '='):
                    // replace placeholder to ensure static if possible
                    $value = StringDataType::replacePlaceholders($value, $placeholder, false);
                    $expression = FormulaFactory::createFromString($this->getWorkbench(), $value);
                    if ($expression->isStatic()) {
                        $row[$column['attribute_alias']] = $expression->evaluate();
                    }
                    break;
                case empty(StringDataType::findPlaceholders($value)) === false:
                    $row[$column['attribute_alias']] = StringDataType::replacePlaceholders($value, $placeholder);
                    break;
                default:
                    $row[$column['attribute_alias']] = $value;
            }
        }

        return $row;
    }

    public function getInternalDatatype($openApiType, $format, $enumValues)
    {
        switch ($openApiType) {
            case 'integer':
                return DataTypeFactory::createFromString($this->getWorkbench(), IntegerDataType::class);

            case 'number':
                return DataTypeFactory::createFromString($this->getWorkbench(), NumberDataType::class);

            case 'boolean':
                return DataTypeFactory::createFromString($this->getWorkbench(), BooleanDataType::class);

            case 'array':
                return DataTypeFactory::createFromString($this->getWorkbench(), ArrayDataType::class);

            case 'string':
                if ($format === 'datetime' || $format === 'date') {
                    return DataTypeFactory::createFromString($this->getWorkbench(), $format === 'datetime' ? DateTimeDataType::class : DateDataType::class);
                }
                if ($format === 'byte' || $format === 'binary') {
                    $binaryType = DataTypeFactory::createFromString($this->getWorkbench(), BinaryDataType::class);
                    if ($binaryType instanceof  BinaryDataType) {
                        $binaryType->setEncoding($format === 'byte' ? 'base64' : 'binary');
                    }
                    return $binaryType;
                }
                if ($format === 'datetime') {
                    return DataTypeFactory::createFromString($this->getWorkbench(), DateTimeDataType::class);
                }
                if ($format === 'date') {
                    return DataTypeFactory::createFromString($this->getWorkbench(), DateDataType::class);
                }
                if ($enumValues !== null) {
                    $enumType = DataTypeFactory::createFromString($this->getWorkbench(), StringEnumDataType::class);
                    // In PowerUi we map keys to the values. In this case, the value also needs to be the key, since the OpenApi definition only has one value for the enum.
                    $enumValues = array_combine($enumValues, $enumValues);
                    if ($enumType instanceof EnumDataTypeInterface) {
                        $enumType->setValues($enumValues);
                    }
                    return $enumType;
                }

                return DataTypeFactory::createFromString($this->getWorkbench(), StringDataType::class);

            default:
                throw new InvalidArgumentException('Openapi schema type: ' . $openApiType . ' not recognized.');
        }
    }

    /**
     * Configure the underlying webservice that provides the OpenApi definition.
     *
     * @uxon-property webservice
     * @uxon-type object
     * @uxon-template {"webservice": {"alias": "service_name", "version": "^1.25.x"}}
     *
     * @param UxonObject $webserviceConfig
     * @return OpenApiExcelToDataSheet
     */
    protected function setWebservice(UxonObject $webserviceConfig) : OpenApiExcelToDataSheet
    {
        $this->webservice = $webserviceConfig->toArray()['webservice'];
        return $this;
    }

    protected function getWebservice() : array
    {
        return $this->webservice;
    }

    /**
     * Add additional columns to the to-object that are filled in every row by this configuration or placeholders.
     *
     * e.g.
     * "additional_rows": [
     *      {
     *           "attribute_alias": "RequestId",
     *           "value": "[#Request#]"
     *      },
     *      {
     *          "attribute_alias": "RequestId",
     *          "value": "=Lookup('UID', 'axenox.ETL.webservice_request', 'flow_run = [#flow_run_uid#]')"
     *      },
     *      {
     *           "attribute_alias": "Betreiber",
     *           "value": "SuedLink"
     *      }
     * ]
     *
     * @uxon-property additional_columns
     * @uxon-type object
     * @uxon-template [{"attribute_alias":"", "value": ""}]
     *
     * @param UxonObject $additionalColumns
     * @return OpenApiJsonToDataSheet
     */
    protected function setAdditionalColumns(UxonObject $additionalColumns) : OpenApiExcelToDataSheet
    {
        $this->additionalColumns = $additionalColumns->toArray();
        return $this;
    }

    protected function getAdditionalColumn() : array
    {
        return $this->additionalColumns;
    }

    /**
     * Define the name of the schema for this specific step.
     * If null, it will try to find the attribute alias within the OpenApi definition.
     *
     * @uxon-property schema_name
     * @uxon-type string
     *
     * @param string $schemaName
     * @return OpenApiJsonToDataSheet
     */
    protected function setSchemaName(string $schemaName) : OpenApiExcelToDataSheet
    {
        $this->schemaName = $schemaName;
        return $this;
    }

    protected function getSchemaName() : ?string
    {
        return $this->schemaName;
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        return new UxonEtlStepResult($stepRunUid, $resultData);
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Interfaces\iCanGenerateDebugWidgets::createDebugWidget()
     */
    public function createDebugWidget(DebugMessage $debug_widget)
    {
        if ($this->baseSheet !== null) {
            $debug_widget = $this->baseSheet->createDebugWidget($debug_widget);
        }
        return $debug_widget;
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::isIncremental()
     */
    public function isIncremental(): bool
    {
        return false;
    }
}