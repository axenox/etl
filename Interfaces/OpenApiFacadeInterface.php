<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\Facades\FacadeInterface;
use Flow\JSONPath\JSONPathException;
use Psr\Http\Message\ServerRequestInterface;

// TODO: turn into subclass of a new AbstractWebserviceType
interface OpenApiFacadeInterface extends FacadeInterface
{
    /**
     * 
     * @param ServerRequestInterface $request
     * @return array|NULL
     */
    public function getOpenApiJson(ServerRequestInterface $request) : ?array;
    
    public function getOpenApiDef(ServerRequestInterface $request) : ?string;

    /**
     * @param ServerRequestInterface $request
     * @param string $jsonPath
     * @param string $contentType
     * @return array
     * @throws JSONPathException
     */
    public function getJsonSchemaFromOpenApiByRef(ServerRequestInterface $request, string $jsonPath, string $contentType): object;

    /**
     *
     * @param ServerRequestInterface $request
     * @return object
     * @throws JSONPathException
     */
    public function getRequestBodySchemaForCurrentRoute(ServerRequestInterface $request) : object;

    /**
     *
     * @param ServerRequestInterface $request
     * @param int $responseCode
     * @return object
     * @throws JSONPathException
     */
    public function getResponseBodySchemaForCurrentRoute(ServerRequestInterface $request, int $responseCode) : object;

    /**
     * @param ServerRequestInterface $request
     * @param string $jsonPath
     * @param string $contentType
     * @return string
     */
    public function findSchemaPathInOpenApiJson(ServerRequestInterface $request, string $jsonPath, string $contentType): string;

    /**
     * Unfortunately the openapi json schema differs from json schema standards, resulting in a necessary conversion
     *
     * @param object $schema
     * @return object
     */
    public function convertNullableToNullType(object $schema) : object;
}