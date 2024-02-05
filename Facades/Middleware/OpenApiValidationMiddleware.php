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
use exface\Core\Exceptions\DataTypes\JsonSchemaValidationError;
use cebe\openapi\spec\OpenApi;
use League\OpenAPIValidation\PSR7\ValidatorBuilder;
use cebe\openapi\ReferenceContext;
use exface\Core\Exceptions\Facades\HttpBadRequestError;
use League\OpenAPIValidation\Schema\Exception\SchemaMismatch;
use League\OpenAPIValidation\Schema\Exception\ContentTypeMismatch;
use League\OpenAPIValidation\Schema\Exception\FormatMismatch;
use League\OpenAPIValidation\Schema\Exception\InvalidSchema;
use League\OpenAPIValidation\Schema\Exception\KeywordMismatch;
use League\OpenAPIValidation\Schema\Exception\NotEnoughValidSchemas;
use League\OpenAPIValidation\Schema\Exception\TooManyValidSchemas;
use League\OpenAPIValidation\Schema\Exception\TypeMismatch;
use GuzzleHttp\Exception\BadResponseException;

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
        } catch (ValidationFailed $exception) {
            $prev = $exception->getPrevious();
            if ($prev) {
                $msg = $prev->getMessage();
                if ($prev instanceof SchemaMismatch) {                	
                	if ($prev->dataBreadCrumb()->buildChain()[0] !== null){
                		$source = ' in `$.' . implode('.', $prev->dataBreadCrumb()->buildChain()) . '`';
                	}
                		
                	$msg = 'Request validation failed' . $source . '. ' . $msg;
                }
            } else {
                $msg = $exception->getMessage();
            }
            throw new HttpBadRequestError($request, $msg, null, $exception);
        }
        
        // 2. Process request
        $response = $handler->handle($request);
        
        // 3. Validate response
        $responseValidator = $builder->getResponseValidator();
        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            try {
                $responseValidator->validate($matchedOASOperation, $response);
            } catch (ValidationFailed $exception) {
            	$prev = $exception->getPrevious();
            	if ($this->isSchemaValidationException($prev)){
            		if ($prev->dataBreadCrumb()->buildChain()[0] !== null){
            			$source = ' in `$.' . implode('.', $prev->dataBreadCrumb()->buildChain()) . '`';
            		}
            		
            		$msg = $prev->getMessage();
            		$msg = 'Response validation failed' . $source . '. ' . $msg;
            		
            		$errorClass = 'exface\Core\Exceptions\DataTypes\JsonSchemaValidationError';
            		throw new $errorClass([$msg], $msg, json: $response->getBody()->__toString());
            	}
            	
            	throw new BadResponseException($msg, $request, null, $exception);
            }
        }
        
        return $response;
    }
    
    private function isSchemaValidationException($exception) : bool
    {
    	switch (true){
    		case $exception instanceof SchemaMismatch:
    		case $exception instanceof ContentTypeMismatch:
    		case $exception instanceof FormatMismatch:
    		case $exception instanceof InvalidSchema:
    		case $exception instanceof KeywordMismatch:
    		case $exception instanceof NotEnoughValidSchemas:
    		case $exception instanceof TooManyValidSchemas:
    		case $exception instanceof TypeMismatch:
    			return true;
    		default:
    			return false;
    	}
    }
    
    protected function getWorkbench() : WorkbenchInterface
    {
        return $this->facade->getWorkbench();
    }
}
