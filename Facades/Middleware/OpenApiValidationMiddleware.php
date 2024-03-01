<?php
namespace axenox\ETL\Facades\Middleware;

use cebe\openapi\exceptions\TypeErrorException;
use cebe\openapi\exceptions\UnresolvableReferenceException;
use League\OpenAPIValidation\PSR7\Exception\Validation\InvalidParameter;
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
use League\OpenAPIValidation\Schema\Exception\InvalidSchema;
use GuzzleHttp\Exception\BadResponseException;

/**
 * This middleware adds request and response validation to facades implementing OpenApiFacadeInterface
 * 
 * @author andrej.kabachnik
 *
 */
final class OpenApiValidationMiddleware implements MiddlewareInterface
{
    private OpenApiFacadeInterface $facade;
    
    private array $excludePatterns = [];
    
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
     *
     * @throws TypeErrorException if request does not contain a valid openapi
     * @throws UnresolvableReferenceException if references within the openapi could not be resolved
     * @throws JsonSchemaValidationError if request  or response did not match json schema
     * @throws HttpBadRequestError if request contained an validation error
     * @throws BadResponseException if response contained an validation error
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
            $msg = $exception->getMessage();
            $prev = $exception->getPrevious();
            if ($prev) {
                $msg = $prev->getMessage();
                switch (true) {
                    case $prev instanceof SchemaMismatch && str_contains($exception->getMessage(), 'Body'):
                        $source = $this->getSource($prev);
                        $context = 'Invalid request body';
                        $msg = $source . '. ' . $msg;
                        $json = json_encode($request->getBody());
                        break;
                    case $prev instanceof InvalidParameter:
                        $schemaError = $prev->getPrevious();
                        $context = 'Invalid request parameter';
                        $msg = $prev->getMessage() . '. ' . $schemaError->getMessage();
                        $json = json_encode($request->getQueryParams());

                }

                throw new JsonSchemaValidationError([$msg], $msg, null, $exception, $context, $json);
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
            	$msg = $exception->getMessage();
            	if ($this->isSchemaValidationException($prev)){
                    $source = $this->getSource($prev);
                    $context = 'Response validation failed';
                    $msg = $source . ': ' . $prev->getMessage();
            		throw new JsonSchemaValidationError([$msg], $msg, null, $exception, $context, $response->getBody()->__toString());
            	}
            	
            	throw new BadResponseException($msg, $request, null, $exception);
            }
        }
        
        return $response;
    }

    private function getSource(\Throwable $exception) : ?string
    {
        if ($exception->dataBreadCrumb()->buildChain()[0] !== null) {
            return 'Invalid input found in `$.'
                . implode('.', $exception->dataBreadCrumb()->buildChain())
                . '`';
        }
    }
    
    private function isSchemaValidationException($exception) : bool
    {
    	switch (true){
    		case $exception instanceof InvalidSchema:
            case $exception instanceof SchemaMismatch:
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
