<?php
namespace axenox\ETL\Facades\Middleware;

use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;
use exface\Core\Interfaces\WorkbenchInterface;
use GuzzleHttp\Psr7\Response;
use exface\Core\Factories\MetaObjectFactory;
use axenox\ETL\Facades\Helper\MetaModelSchemaBuilder;

/**
 * This middleware creates a valid OpenApi JSON from the request and webservice parameter.
 * 
 * @author miriam.seitz
 *
 */
final class OpenApiMiddleware implements MiddlewareInterface
{
    private $facade = null;
    
    private $headers;
    
    private $routePattern;
    
    public function __construct(OpenApiFacadeInterface $facade, array $headers, string $routePattern)
    {
    	$this->facade = $facade;
    	$this->headers = $headers;
        $this->routePattern = $routePattern;
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
        if (preg_match($this->routePattern, $path) === 0) {
        	return $handler->handle($request);
        }
        
        $openApiJson = $this->facade->getOpenApiJson($request);	    
        if ($openApiJson === null) {
            return $handler->handle($request);
        }
        
        // building functional OpenApi
        $openApiJson = $this->autogenerateMetamodelSchemas($openApiJson);
        $openApiJson = json_encode($openApiJson, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        
        $headers = array_merge($this->headers, ['Content-Type' => 'application/json']);
        $response = new Response(200, $headers, $openApiJson);
        return $response;
    }
    
    /**
     * Add requested metamodel schemas into openapi components
     * 
     * @param array $swaggerArray
     */
    private function autogenerateMetamodelSchemas(array $swaggerArray)
    {
    	$swaggerSchema = $swaggerArray['components']['schemas'];
    	if (array_key_exists('Metamodel Informationen', $swaggerSchema)){
    		if (array_key_exists('Metamodel Informationen', $swaggerSchema)) {
    			$attribtueAliasesToAdd = array_keys($swaggerSchema['Metamodel Informationen']['properties']);
    			foreach ($attribtueAliasesToAdd as $metaobjectAlias){
    				$metaObjectSchema = MetaModelSchemaBuilder::transformIntoJsonSchema(
    					MetaObjectFactory::createFromString($this->getWorkbench(), $metaobjectAlias),
    					$attribtueAliasesToAdd);
    				foreach ($attribtueAliasesToAdd as $metaobjectAlias) {
    					$metaObjectSchema = MetaModelSchemaBuilder::transformIntoJsonSchema(
    						MetaObjectFactory::createFromString($this->getWorkbench(), $metaobjectAlias), 
    						$attribtueAliasesToAdd);
    					
    					$swaggerSchema['Metamodel Informationen']['properties'][$metaobjectAlias] = $metaObjectSchema[$metaobjectAlias];
    				}
    			}
    		}
    		
    		$swaggerArray['components']['schemas']['Metamodel Informationen'] = $swaggerSchema;
    	}
    	
    	return $swaggerArray;
    }
    
    protected function getWorkbench() : WorkbenchInterface
    {
        return $this->facade->getWorkbench();
    }
}
