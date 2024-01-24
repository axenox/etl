<?php
namespace axenox\ETL\Facades\Middleware;

use League\OpenAPIValidation\PSR15\Exception\InvalidResponseMessage;
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
use League\OpenAPIValidation\Schema\Exception\SchemaMismatch;

/**
 * This middleware adds request and response validation to facades implementing OpenApiFacadeInterface
 * 
 * @author andrej.kabachnik
 *
 */
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
        
        $schema = new OpenApi($swaggerArray);
        $schema->resolveReferences(new ReferenceContext($schema, "/"));
        
        $builder = (new ValidatorBuilder())->fromSchema($schema);
        $requestValidator = $builder->getRequestValidator();
        
        // 1. Validate request
        try {
            $matchedOASOperation = $requestValidator->validate($request);
        } catch (ValidationFailed $e) {
            $prev = $e->getPrevious();
            if ($prev) {
                $msg = $prev->getMessage();
                if ($prev instanceof SchemaMismatch) {
                    $msg = 'Request validation failed in `$.' . implode('.', $prev->dataBreadCrumb()->buildChain()) . '`. ' . $msg;
                }
            } else {
                $msg = $e->getMessage();
            }
            throw new HttpBadRequestError($request, $msg, null, $e);
        }
        
        // 2. Process request
        $response = $handler->handle($request);
        
        // 3. Validate response
        $responseValidator = $builder->getResponseValidator();
        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            try {
                // TODO currently seems not po pass content-type validation for application/json
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
