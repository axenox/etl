<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractOpenApiPrototype;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\DataTypes\ArrayDataType;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\Exceptions\NotImplementedError;
use exface\Core\Factories\FormulaFactory;
use exface\Core\Factories\DataSheetFactory;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\Tasks\HttpTaskInterface;
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use Flow\JSONPath\JSONPath;
use Flow\JSONPath\JSONPathException;
use axenox\ETL\Common\UxonEtlStepResult;

/**
 * Objects have to be defined with an x-object-alias and with x-attribute-aliases like:
 * ´´´
 * {
 *     "Object": {
 *          "type": "object",
 *          "x-object-alias": "alias",
 *          "properties: {
 *              "Id" {
 *                  "type": "string",
 *                  "x-attribute-alias": "UID"
 *              }
 *          }
 *     }
 * }
 *
 * ´´´
 *
 * Only use direct Attribute aliases in the definition and never relation paths or formulars!
 * e.g "x-attribute-alias": "Objekt_ID"
 * If you want to link objects, use the id/uid in the original attribute.
 * e.g. "x-attribute-alias": "Request" -> '0x11EFBD3FD893913ABD3F005056BEF75D'
 *
 * The from-object HAS to be defined within the request schema of the route to the step!
 * e.g. with multiple structural concepts
 * "requestBody": {
 *   "description": "Die zu importierenden Daten im Json format.",
 *   "required": true,
 *   "content": {
 *     "application/json": {
 *       "schema": {
 *         "type": "object",
 *         "properties": {
 *           "Objekte": {
 *             "type": "array",
 *             "items": {
 *               "$ref": "#/components/schemas/Object"
 *             },
 *             "x-object-alias": "full.namespace.Object"
 *           }
 *         }
 *       }
 *     }
 *   }
 * }
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
class OpenApiJsonToDataSheet extends AbstractOpenApiPrototype
{
    private $additionalColumns = null;
    private $schemaName = null;

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
        $stepTask = $stepData->getTask();

        if ($stepTask instanceof HttpTaskInterface === false){
            throw new InvalidArgumentException('Http request needed to process OpenApi definitions! `' . get_class($stepTask) . '` received instead.');
        }
        
        $this->baseSheet = $baseSheet;
        $this->getWorkbench()->eventManager()->dispatch(new OnBeforeETLStepRun($this));

        $requestLogData = $this->loadRequestData($stepData, ['http_body', 'http_content_type'])->getRow();
        $requestBody = json_decode($requestLogData['http_body'], true);

        if ($requestLogData['http_content_type'] !== 'application/json' || $requestBody === null) {
            yield 'No HTTP content found to process' . PHP_EOL;
            return $result->setProcessedRowsCounter(0);
        }

        $toSheet = $baseSheet->copy();

        $openApiJson = $this->getOpenApiJson($stepData->getTask());
        $schemas = (new JSONPath(json_decode($openApiJson, false)))
            ->find(self::JSON_PATH_TO_OPEN_API_SCHEMAS)->getData()[0];
        $schemas = json_decode(json_encode($schemas), true);

        if (($schemaName = $this->getSchemaName()) !== null && array_key_exists($schemaName, $schemas)) {
            $toObjectSchema = $schemas[$schemaName];
        } else {
            $toObjectSchema = $this->findObjectSchema($toSheet, $schemas);
        }

        $jsonPath = '$.paths.[#routePath#].[#methodType#].requestBody.content.[#ContentType#].schema';
        $requestSchema = $this->getSchema($stepTask->getHttpRequest(), $openApiJson, $jsonPath);

        if ($requestSchema === null) {
            throw new InvalidArgumentException('Cannot find necessary request schema in OpenApi. Please check the OpenApi definition!´.'
                . $jsonPath
                . '´ Please check the OpenApi definition!');
        }

        $key = $this->getArrayKeyToImportDataFromSchema($requestSchema, $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS]);
        $dsToImport = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS]);
        $dsToImport->getColumns()->addFromSystemAttributes();
        $this->fillDataSheetWithImportData($dsToImport, $requestBody, $toObjectSchema, $placeholders, $key);

        // Saving relations is very complex and not yet supported for OpenApi Imports
        $this->removeRelationColumns($dsToImport);

        yield 'Importing rows ' . $dsToImport->countRows() . ' for ' . $toSheet->getMetaObject()->getAlias(). ' with the data sent via webservice request.';

        $transaction = $this->getWorkbench()->data()->startTransaction();

        try {
            // we only create new data in import, either there is an import table or a PreventDuplicatesBehavior
            // that can be used to update known entire
            $dsToImport->dataCreate(false, $transaction);
        } catch (\Throwable $e) {
            $transaction->rollback();
            throw $e;
        }

        $transaction->commit();
        return $result->setProcessedRowsCounter($toSheet->countRows());
    }

    /**
     * Searches through the request schema looking for the object reference and returning its name.
     * This key can than be used to find the object within the request body.
     *
     * @param array $requestSchema
     * @param string $objectAlias
     * @return string|null
     */
    protected function getArrayKeyToImportDataFromSchema(array $requestSchema, string $objectAlias) : ?string
    {
        $key = null;
        switch ($requestSchema['type']) {
            case 'array':
                $key = $this->getArrayKeyToImportDataFromSchema($requestSchema['items'], $objectAlias);
                break;
            case 'object':
            foreach ($requestSchema['properties'] as $propertyName => $propertyValue) {
                switch (true) {
                    case array_key_exists('x-object-alias', $propertyValue) && $propertyValue['x-object-alias'] === $objectAlias:
                        return $propertyName;
                        break;
                    case $propertyValue['type'] === 'array':
                    case $propertyValue['type'] === 'object':
                        $key = $this->getArrayKeyToImportDataFromSchema($propertyValue, $objectAlias);
                        break;
                }
            }
        }

        return $key;
    }

    /**
     * Reads import data from the request body. If no key is specified, it will search the response body for the right object.
     * Otherwise, it will try to read the whole request body content into the requested datasheet for the object of this step.
     *
     * @param mixed $requestBody
     * @param array $toObjectSchema
     * @param array $placeholder
     * @param DataSheetInterface $dataSheet
     * @param string|null $key
     * @return void
     */
    protected function fillDataSheetWithImportData(
        DataSheetInterface $dataSheet,
        array $requestBody,
        array $toObjectSchema,
        array $placeholder,
        ?string $key = null) : void
    {
        $attributeAliasByPropertyName = [];
        foreach ($toObjectSchema['properties'] as $propertyName => $propertyValue) {
            $attributeAliasByPropertyName[$propertyName] = $propertyValue[self::OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_ALIAS];
        }

        $importData = [];
        // Determine if the request body contains a named array/object or an unnamed array/object
        $sourceData = is_array($requestBody[$key]) ? $requestBody[$key] : $requestBody;

        if (ArrayDataType::isSequential($sourceData)) {
            // Named array: { "object-key" [ {"id": "123", "name": "abc" }, {"id": "234", "name": "cde"} ] }
            // Unnamed array: [ {"id": "123", "name": "abc" }, {"id": "234", "name": "cde"} ]
            $rowIndex = 0;
            foreach ($sourceData as $entry) {
                $row = $this->getImportDataFromRequestBody($entry, $attributeAliasByPropertyName);
                $this->addRowToDataSheetWithAdditionalColumns($dataSheet, $placeholder, $row, $rowIndex);
                $rowIndex++;
            }
        } else {
            // Named object: { "object-key" {"id": "123", "name": "abc" } }
            // Unnamed object: {"id": "123", "name": "abc" }
            $row = $this->getImportDataFromRequestBody($sourceData, $attributeAliasByPropertyName);
            $this->addRowToDataSheetWithAdditionalColumns($dataSheet, $placeholder, $row, 0);
        }
    }

    /**
     * @param $requestBody
     * @param $attributeAliasByPropertyName
     * @return array
     */
    protected function getImportDataFromRequestBody($requestBody, $attributeAliasByPropertyName) : array
    {
        $importData = [];
        foreach ($requestBody as $propertyName => $value) {
            switch(true) {
                case array_key_exists($propertyName, $attributeAliasByPropertyName) === false && is_array($value):
                case is_numeric($propertyName):
                    $importData = $this->getImportDataFromRequestBody($value, $attributeAliasByPropertyName);
                    break;
                case array_key_exists($propertyName, $attributeAliasByPropertyName):
                    // arrays and objects are represented via string in the database
                    if (is_array($value)) {
                        $value =  trim(json_encode($value), '[]');
                    }

                    if (is_object($value)) {
                        $value =  json_encode($value);
                    }

                    $importData[$attributeAliasByPropertyName[$propertyName]] = $value;
                    break;
            }
        }

        return $importData;
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
    protected function setAdditionalColumns(UxonObject $additionalColumns) : OpenApiJsonToDataSheet
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
    protected function setSchemaName(string $schemaName) : OpenApiJsonToDataSheet
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