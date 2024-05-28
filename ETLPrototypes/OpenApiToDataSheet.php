<?php
namespace axenox\ETL\ETLPrototypes;

use axenox\ETL\Common\AbstractOpenApiPrototype;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\Factories\FormulaFactory;
use exface\Core\Factories\DataSheetFactory;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\DataTypes\StringDataType;
use axenox\ETL\Common\IncrementalEtlStepResult;
use exface\Core\Interfaces\Tasks\HttpTaskInterface;
use exface\Core\Widgets\DebugMessage;
use axenox\ETL\Events\Flow\OnBeforeETLStepRun;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use Flow\JSONPath\JSONPath;
use Flow\JSONPath\JSONPathException;

/**
 * Objects have to be defined with an x-object-alias and with x-attribute-aliases like:
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
class OpenApiToDataSheet extends AbstractOpenApiPrototype
{
    private $additionalColumns = null;

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
    	$result = new IncrementalEtlStepResult($stepRunUid);
        $stepTask = $stepData->getTask();

        if ($stepTask instanceof HttpTaskInterface === false){
            throw new InvalidArgumentException('Http request needed to process OpenApi definitions! Request type: ' . get_class($stepTask));
        }
        
        $this->baseSheet = $baseSheet;
        $this->getWorkbench()->eventManager()->dispatch(new OnBeforeETLStepRun($this));

        $requestLogData = $this->loadRequestData($stepData, ['http_body', 'http_content_type']);
        $requestBody = json_decode($requestLogData['http_body'], true);
        $toSheet = $baseSheet->copy();

        $openApiJson = $stepData->getOpenApiJson();
        $schemas = (new JSONPath(json_decode($openApiJson, false)))
            ->find(self::JSON_PATH_TO_OPEN_API_SCHEMAS)->getData()[0];
        $schemas = json_decode(json_encode($schemas), true);

        $toObjectSchema = $this->findObjectSchema($toSheet, $schemas);
        $requestSchema = $this->getSchema($stepTask->getHttpRequest(), $openApiJson, '$.paths.[#routePath#].[#methodType#].requestBody.content.[#ContentType#].schema');
        $key = $this->getArrayKeyToImportDataFromSchema($requestSchema, $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS]);
        $importData = $this->getImportData($requestBody, $key, $toObjectSchema, $placeholders);
        $dsToImport = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), $toObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS]);
        $dsToImport->getColumns()->addFromSystemAttributes();

        yield 'Importing rows ' . count($importData) . ' for ' . $toSheet->getMetaObject()->getAlias(). ' with the data sent via webservice request.';

        $transaction = $this->getWorkbench()->data()->startTransaction();
        $dsToImport->addRows($importData);

        try {
            $dsToImport->dataUpdate(true, $transaction);
        } catch (\Throwable $e) {
            $transaction->rollback();
            throw $e;
        }

        $transaction->commit();
        return $result->setProcessedRowsCounter($toSheet->countRows());
    }

    public function validate(): \Generator
    {
        yield from [];
    }

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

    protected function getImportData(mixed $requestBody, string $key, array $toObjectSchema, array $placeholder) : array
    {
        $importData = [];

        if (array_key_exists($key, $requestBody) === false) {
            throw new InvalidArgumentException('Cannot find import data in request body!');
        }

        $attributeAliasByPropertyName = [];
        foreach ($toObjectSchema['properties'] as $propertyName => $propertyValue) {
            $attributeAliasByPropertyName[$propertyName] = $propertyValue[self::OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_ALIAS];
        }

        $importData = [];
        if (is_array($requestBody[$key])) {
            foreach ($requestBody[$key] as $entry) {
                $row = $this->getImportDataFromRequestBody($entry, $attributeAliasByPropertyName);
                $this->addAdditionalColumnsToRow($placeholder, $row);
                $importData[] = $row;
            }
        } else {
            $row = $this->getImportDataFromRequestBody($requestBody, $attributeAliasByPropertyName);
            $this->addAdditionalColumnsToRow($placeholder, $row);
            $importData[] = $row;
        }

        return $importData;
    }

    /** @noinspection PhpDuplicateSwitchCaseBodyInspection */
    protected function getImportDataFromRequestBody($requestBody, $attributeAliasByPropertyName)
    {
        foreach ($requestBody as $propertyName => $value) {
            switch(true) {
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
                // must be last to check because array can be the type of the value if found!
                case is_array($value):
                    $importData = $this->getImportDataFromRequestBody($value, $attributeAliasByPropertyName);
                    break;
            }
        }

        return $importData;
    }

    /**
     * @param array $placeholder
     * @param $row
     * @return mixed
     */
    public function addAdditionalColumnsToRow(array $placeholder, array &$row) : void
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
                case empty($phs = StringDataType::findPlaceholders($value)) === false:
                    $row[$column['attribute_alias']] = StringDataType::replacePlaceholders($value, $placeholder);
                    break;
                default:
                    $row[$column['attribute_alias']] = $value;
            }
        }
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
     * @return OpenApiToDataSheet
     */
    protected function setAdditionalColumns(UxonObject $additionalColumns) : OpenApiToDataSheet
    {
        $this->additionalColumns = $additionalColumns->toArray();
        return $this;
    }

    protected function getAdditionalColumn() : array
    {
        return $this->additionalColumns;
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\ETLStepInterface::parseResult()
     */
    public static function parseResult(string $stepRunUid, string $resultData = null): ETLStepResultInterface
    {
        return new IncrementalEtlStepResult($stepRunUid, $resultData);
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