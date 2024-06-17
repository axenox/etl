<?php

namespace axenox\ETL\Common;

use axenox\ETL\Common\AbstractETLPrototype;
use axenox\ETL\Interfaces\ETLStepDataInterface;
use axenox\ETL\Interfaces\ETLStepResultInterface;
use exface\Core\CommonLogic\DataSheets\DataSheet;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Widgets\DebugMessage;
use Flow\JSONPath\JSONPath;
use Flow\JSONPath\JSONPathException;
use Psr\Http\Message\ServerRequestInterface;

abstract class AbstractOpenApiPrototype extends AbstractETLPrototype
{
    const JSON_PATH_TO_OPEN_API_SCHEMAS = '$.components.schemas';

    const OPEN_API_ATTRIBUTE_TO_OBJECT_ALIAS = 'x-object-alias';

    const OPEN_API_ATTRIBUTE_TO_ATTRIBUTE_ALIAS = 'x-attribute-alias';

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
     * Finds success response of the current route in the given OpenApi json.
     *
     * @param ServerRequestInterface $request
     * @param string $openApiJson
     * @param string $jsonPath
     * @return array
     * @throws JSONPathException
     */
    function getSchema(ServerRequestInterface $request, string $openApiJson, string $jsonPath) : array
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

        $path = $request->getUri()->getPath();
        $path = StringDataType::substringAfter($path, 'dataflow' . '/', '');
        $routePath = rtrim(strstr($path, '/'), '/');
        $methodType = strtolower($request->getMethod());
        $contentType = $request->getHeader('Content-Type')[0];
        $jsonPath = str_replace(
            ['[#routePath#]', '[#methodType#]', '[#ContentType#]'],
            [$routePath, $methodType, $contentType],
            $jsonPath);
        $jsonPathFinder = new JSONPath(json_decode($openApiJson, false));
        $data = $jsonPathFinder->find($jsonPath)->getData()[0];

        if ($data === null) {
            throw new InvalidArgumentException('Cannot find request schema in OpenApi. Please check the route definition!');
        }

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
}