<?php

namespace axenox\ETL\Common;

use axenox\ETL\Common\AbstractETLPrototype;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\CommonLogic\DataSheets\DataSheet;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\Facades\AbstractHttpFacade\Middleware\RouteConfigLoader;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Factories\FormulaFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Widgets\DebugMessage;
use Flow\JSONPath\JSONPath;
use Flow\JSONPath\JSONPathException;
use Psr\Http\Message\ServerRequestInterface;
use exface\Core\Interfaces\Tasks\TaskInterface;
use exface\Core\Interfaces\Tasks\HttpTaskInterface;
use exface\Core\Exceptions\RuntimeException;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;

abstract class AbstractOpenApiPrototype extends AbstractETLPrototype
{
    const JSON_PATH_TO_OPEN_API_SCHEMAS = '$.components.schemas';
    const OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS = 'x-object-alias';
    const OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_ALIAS = 'x-attribute-alias';
    const OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_CALCULATION = 'x-attribute-calculation';
    const OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_DATAADDRESS = 'x-attribute-dataaddress';

    const OPEN_API_ATTRIBUTE_TO_DATATYPE = 'type';
    const OPEN_API_ATTRIBUTE_TO_FORMAT = 'format';
    const OPEN_API_ATTRIBUTE_TO_ENUM_VALUES = 'enum';

    protected $baseSheet = null;

    /**
     * Finds the object schema by mapping the datasheet to the ´x-object-alias´ in the OpenApi schema.
     *
     * @param DataSheetInterface $dataSheet
     * @param array $schemas
     * @return array
     * @throws InvalidArgumentException
     */
    protected function findObjectSchema(DataSheetInterface $dataSheet, array $schemas): array
    {
        switch(true) {
            case array_key_exists($dataSheet->getMetaObject()->getAliasWithNamespace(), $schemas):
                $fromObjectSchema = $schemas[$dataSheet->getMetaObject()->getAliasWithNamespace()];
                break;
            case array_key_exists($dataSheet->getMetaObject()->getAlias(), $schemas):
                $fromObjectSchema = $schemas[$key[0] ?? $dataSheet->getMetaObject()->getAlias()];

                if ($fromObjectSchema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS] !== $dataSheet->getMetaObject()->getAliasWithNamespace()) {
                    throw new InvalidArgumentException('From sheet does not match ' .
                        self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS .
                        ' of found schema in the OpenApi definition!');
                }
                break;
            default:
                foreach ($schemas as $schema) {
                    if ($schema[self::OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS] === $dataSheet->getMetaObject()->getAliasWithNamespace()) {
                        return $schema;
                    }
                }

                throw new InvalidArgumentException('From object not found in OpenApi schema!');
        }

        return $fromObjectSchema;
    }
    
    /**
     * 
     * @param TaskInterface $task
     * @throws InvalidArgumentException
     * @return string
     */
    protected function getOpenApiJson(TaskInterface $task) : string
    {
        if (! ($task instanceof HttpTaskInterface)) {
            throw new InvalidArgumentException('Cannot use OpenAPI flow steps with non-HTTP tasks!');
        }
        
        $facade = $task->getFacade();
        if ($facade === null || ! ($facade instanceof OpenApiFacadeInterface)) {
            throw new InvalidArgumentException('Cannot use OpenAPI flow steps with non-OpenAPI facades!');
        }
        
        $json = $facade->getOpenApiDef($task->getHttpRequest());
        if ($json === null) {
            throw new InvalidArgumentException('Cannot load OpenAPI definition from HTTP task!');
        }
        return $json;
    }

    /**
     * Finds success response of the current route in the given OpenApi json.
     *
     * @param ServerRequestInterface $request
     * @param string $openApiJson
     * @param string $jsonPath
     * @return array
     * @throws JSONPathException
     */
    function getSchema(ServerRequestInterface $request, string $openApiJson, string &$jsonPath) : ?array
    {
        // Use local version of JSONPathLexer with edit to
        // Make sure to require BEFORE the JSONPath classes are loaded, so that the custom lexer replaces
        // the one shipped with the library.
        require_once '..' . DIRECTORY_SEPARATOR
            . '..' . DIRECTORY_SEPARATOR
            . 'axenox' . DIRECTORY_SEPARATOR
            . 'etl' . DIRECTORY_SEPARATOR
            . 'Common' . DIRECTORY_SEPARATOR
            . 'JSONPath' . DIRECTORY_SEPARATOR
            . 'JSONPathLexer.php';

        $routePath = RouteConfigLoader::getRoutePath($request);
        $methodType = strtolower($request->getMethod());
        $contentType = $request->getHeader('Content-Type')[0];
        $jsonPath = str_replace(
            ['[#routePath#]', '[#methodType#]', '[#ContentType#]'],
            [$routePath, $methodType, $contentType],
            $jsonPath);
        $jsonPathFinder = new JSONPath(json_decode($openApiJson, false));
        $data = $jsonPathFinder->find($jsonPath)->getData()[0];
        return json_decode(json_encode($data), true);
    }


    /**
     * @param ETLStepDataInterface $stepData
     * @param array $requestedColumns
     * @return DataSheet|DataSheetInterface
     */
    protected function loadRequestData(ETLStepDataInterface $stepData, array $requestedColumns): DataSheet|DataSheetInterface
    {
        $requestLogData = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice_request');
        $requestLogData->getColumns()->addFromSystemAttributes();
        $requestLogData->getColumns()->addMultiple($requestedColumns);
        $requestLogData->getFilters()->addConditionFromString('flow_run', $stepData->getFlowRunUid());
        $requestLogData->dataRead();

        if ($requestLogData->countRows() > 1) {
            throw new InvalidArgumentException('Ambiguous web requests!');
        }

        return $requestLogData;
    }

    /**
     * Adds additional data provided by the ´additional_rows´ config within the step to given row into the given datasheet.
     *
     * @param DataSheetInterface $dataSheet
     * @param array $placeholder
     * @param array $row
     * @param int $rowIndex
     * @return void
     */
    protected function addRowToDataSheetWithAdditionalColumns(DataSheetInterface $dataSheet, array $placeholder, array $row, int $rowIndex) : void
    {
        $additionalColumn = $this->getAdditionalColumn();
        // add row data to placeholders so they can be used in formulars
        $placeholder = array_merge($placeholder, $row);
        $dataSheet->addRow($row);
        foreach ($additionalColumn as $column) {
            $value = $column['value'];
            switch (true) {
                case str_contains($value, '='):
                    // replace placeholder to ensure static if possible
                    $value = StringDataType::replacePlaceholders($value, $placeholder, false);
                    $expression = FormulaFactory::createFromString($this->getWorkbench(), $value);
                    $dataSheet->setCellValue($column['attribute_alias'], $rowIndex,  $expression->evaluate($dataSheet, $rowIndex));
                    break;
                case empty(StringDataType::findPlaceholders($value)) === false:
                    $dataSheet->setCellValue($column['attribute_alias'], $rowIndex,  StringDataType::replacePlaceholders($value, $placeholder));
                    break;
                default:
                    $dataSheet->setCellValue($column['attribute_alias'], $rowIndex, $value);
            }
        }
    }

    /**
     * Datasheets from OpenApi JSON data cannot have relations.
     * There are only used for dynamic formulars like =Lookup()
     * and must be removed when the input data has been processed into the datasheet.
     *
     * @param DataSheetInterface $dataSheet
     * @return void
     */
    protected function removeRelationColumns(DataSheetInterface $dataSheet): void
    {
        foreach ($dataSheet->getColumns() as $column) {
            if ($column->getAttribute() === false) {
                throw new InvalidArgumentException('Cannot find Attribute with alias \'' . $column->getName()
                    . '\'  in object \'' . $dataSheet->getMetaObject()->getName() . '\'');
            }

            if ($column->getAttribute()->getObject()->isExactly($dataSheet->getMetaObject()) == false) {
                $dataSheet->getColumns()->remove($column);
            }
        }
    }
}