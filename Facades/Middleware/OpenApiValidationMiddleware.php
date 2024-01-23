<?php
namespace axenox\ETL\Facades\Middleware;

use League\OpenAPIValidation\PSR15\Exception\InvalidResponseMessage;
use League\OpenAPIValidation\PSR15\Exception\InvalidServerRequestMessage;
use League\OpenAPIValidation\PSR7\Exception\ValidationFailed;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;
use exface\Core\Interfaces\WorkbenchInterface;
use cebe\openapi\spec\OpenApi;
use League\OpenAPIValidation\PSR7\ValidatorBuilder;
use cebe\openapi\ReferenceContext;
use exface\Core\Exceptions\Facades\HttpBadRequestError;

final class OpenApiValidationMiddleware implements MiddlewareInterface
{
    private $facade = null;
    
    private $routeDataAttributeName = null;
    
    private $schemaColumnName = null;
    
    private $excludePatterns = [];
    
    public function __construct(OpenApiFacadeInterface $facade, array $excludePatterns = [])
    {
        $this->facade = $facade;
        $this->excludePatterns = $excludePatterns;
    }
    
    /**
     * Process an incoming server request.
     *
     * Processes an incoming server request in order to produce a response.
     * If unable to produce the response itself, it may delegate to the provided
     * request handler to do so.
     */
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $path = $request->getUri()->getPath();
        foreach ($this->excludePatterns as $pattern) {
            if (preg_match($pattern, $path) === 1) {
                return $handler->handle($request);
            }
        }
        
        $swaggerArray = $this->facade->getOpenApiJson($request);
        if ($swaggerArray === null) {
            return $handler->handle($request);
        }
        /*
        $spec = new OpenApi($swaggerArray);
        # (optional) reference resolving
        $spec->resolveReferences(new ReferenceContext($spec, "/"));
        $schema = new Schema($spec->schema);
        */
        $schema = new OpenApi($swaggerArray);
        $schema->resolveReferences(new ReferenceContext($schema, "/"));
        /*
        if ($schema instanceof \cebe\openapi\DocumentContextInterface) {
            $schema->setDocumentContext($schema, new \cebe\openapi\json\JsonPointer(''));
        }*/
        $builder = (new ValidatorBuilder())->fromSchema($schema);
        $requestValidator = $builder->getRequestValidator();
        
        // 1. Validate request
        try {
            $matchedOASOperation = $requestValidator->validate($request);
        } catch (ValidationFailed $e) {
            $msg = 'Request validation failed in `$.' . implode('.', $e->getPrevious()->dataBreadCrumb()->buildChain()) . '`. ' .$e->getPrevious()->getMessage();
            throw new HttpBadRequestError($request, $msg, null, $e);
            // throw InvalidServerRequestMessage::because($e);
        }
        
        // 2. Process request
        $response = $handler->handle($request);
        
        // 3. Validate response
        $responseValidator = $builder->getResponseValidator();
        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            try {
                // $responseValidator->validate($matchedOASOperation, $response);
            } catch (ValidationFailed $e) {
                throw InvalidResponseMessage::because($e);
            }
        }
        
        return $response;
    }
    
    protected function getWorkbench() : WorkbenchInterface
    {
        return $this->facade->getWorkbench();
    }
}
