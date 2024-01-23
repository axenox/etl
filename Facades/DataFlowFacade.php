<?php
namespace axenox\ETL\Facades;

use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use axenox\ETL\Actions\RunETLFlow;
use axenox\ETL\DataTypes\WebRequestStatusDataType;
use exface\Core\CommonLogic\Selectors\ActionSelector;
use exface\Core\CommonLogic\Tasks\HttpTask;
use exface\Core\DataTypes\JsonDataType;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Exceptions\InternalError;
use exface\Core\Exceptions\Facades\FacadeRoutingError;
use exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade;
use exface\Core\Facades\AbstractHttpFacade\Middleware\RouteConfigLoader;
use exface\Core\Factories\ActionFactory;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Factories\MetaObjectFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\Exceptions\ExceptionInterface;
use exface\Core\Interfaces\Tasks\ResultInterface;
use JsonSchema\Validator;
use Flow\JSONPath\JSONPath;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\Exceptions\InvalidArgumentException;
use exface\Core\DataTypes\IntegerDataType;
use exface\Core\DataTypes\NumberDataType;
use exface\Core\DataTypes\ArrayDataType;
use exface\Core\Interfaces\DataTypes\EnumDataTypeInterface;
use exface\Core\DataTypes\DateTimeDataType;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\DataTypes\BinaryDataType;
use exface\Core\DataTypes\DateDataType;

/**
 * 
 * 
 * @author Andrej Kabachnik
 * 
 */
class DataFlowFacade extends AbstractHttpFacade
{
    const REQUEST_ATTRIBUTE_NAME_ROUTE = 'route';

    /**
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::createResponse()
     */
    protected function createResponse(ServerRequestInterface $request) : ResponseInterface
    {
    	$headers = $this->buildHeadersCommon();

        try {
            $path = $request->getUri()->getPath();
            $path = StringDataType::substringAfter($path, $this->getUrlRouteDefault() . '/', '');
            $routePath = rtrim(strstr($path, '/'), '/');
            $routeModel = $this->getRouteData($request);
            $requestLogData = $this->logRequestReceived($request);

            // validate webservice swagger
            $response = $this->getSwaggerValidatorResponse($routeModel, $requestLogData, $headers);
            if ($response !== null){
            	return $response;
            }

            // handle route requests
            switch(true){
                case mb_stripos($path, '/swaggerui') !== false:
                    $content = $this->buildHtmlSwaggerUI('openapi.json');
                    return new Response(200, $this->buildHeadersCommon(), $content);
                    
            	// webservice maintenance requests
            	case mb_stripos($path, '/openapi') !== false && $request->getMethod() === 'GET':
            		// building functional OpenApi
        		    $swaggerArray = json_decode($routeModel['swagger_json'], true);
        		    $this->addServerPaths($path, $swaggerArray);                    
        		    $this->autogenerateMetamodelSchemas($swaggerArray);                    
                    $swaggerJson = json_encode($swaggerArray, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
                    
                    $headers = array_merge($headers, ['Content-Type' => 'application/json']);
        		    $response = new Response(200, $headers, $swaggerJson);
        			return $response;        			
            	// webservice dataflow request
            	default:            	    
            	    $routeUID = $routeModel['UID'];
            		$flowAlias = $routeModel['flow__alias'];
            		$flowRunUID = RunETLFlow::generateFlowRunUid();
            		$requestLogData = $this->logRequestProcessing($requestLogData, $routeUID, $flowRunUID); // flow data update
            		$flowResult = $this->runFlow($flowAlias, $request, $requestLogData); // flow data update
            		$flowOutput = $flowResult->getMessage();
            		
            		// get changes by flow
            		$this->reloadRequestData($requestLogData);

            		if ($requestLogData->countRows() == 1){
            			$headers['Content-Type'] = 'application/json';
            			if (empty($requestLogData->getRow()['response_body'])){
            				$methodType = strtolower($request->getMethod());
            				$jsonPath = $routeModel['type__default_response_path'];
            				if ($jsonPath) {
            				    $body = $this->createEmptyRequestBodyFromSwaggerJson($routePath, $methodType, $jsonPath, $routeModel['swagger_json']);
            				} else {
            				    $body = null;
            				}
            			} else {
	            			$body = $requestLogData->getRow()['response_body'];
            			}

            			$response = new Response(200, $headers, $body);
            		}
            }

        } catch (\Throwable $e) {
        	// get changes by flow
            if ($requestLogData === null) {
                $requestLogData = $this->logRequestReceived($request);
            }
            $this->reloadRequestData($requestLogData);
            
            if (! $e instanceof ExceptionInterface){
                $e = new InternalError($e->getMessage(), null, $e);
            }
            
        	$response = $this->createResponseFromError($e, $request, $requestLogData);
        	$requestLogData = $this->logRequestFailed($requestLogData, $e, $response);
            return $response;
        }

        if ($response === null) {
			$response = new Response(200, $headers, 'Dataflow successfull.');
        }

        $requestLogData = $this->logRequestDone($requestLogData, $flowOutput, $response);
        return $response;
    }
    
	/**
	 * @param string $path
	 * @param array $swaggerArray
	 */
	 private function addServerPaths(string &$path, array &$swaggerArray)
	 {
	 	$basePath = $this->getUrlRouteDefault();
	 	$webserviceBase = StringDataType::substringBefore($path, '/', '', true, true) . '/';
	 	$basePath .= '/' . $webserviceBase;
		foreach ($this->getWorkbench()->getConfig()->getOption('SERVER.BASE_URLS') as $baseUrl) {
		    $swaggerArray['servers'][] = ['url' => $baseUrl . '/' . $basePath];
		}
	 }

	/**
	 * @param array $swaggerArray
	 */
	private function autogenerateMetamodelSchemas(array &$swaggerArray)
	{
		$swaggerSchema = &$swaggerArray['components']['schemas'];
		if (array_key_exists('Metamodel Informationen', $swaggerSchema)){
			$attribtueAliasesToAdd = array_keys($swaggerSchema['Metamodel Informationen']['properties']);
			foreach ($attribtueAliasesToAdd as $metaobjectAlias){
				$metaObjectSchema = $this->transformIntoJsonSchema(
					MetaObjectFactory::createFromString($this->getWorkbench(), $metaobjectAlias),
					$attribtueAliasesToAdd);
				$swaggerSchema['Metamodel Informationen']['properties'][$metaobjectAlias] = $metaObjectSchema[$metaobjectAlias];
			}
		}
	}

	/**
	 * @param array $routeModel
	 * @param ServerRequestInterface $requestLogData
	 * @param array $headers
	 */
	private function getSwaggerValidatorResponse(array $routeModel, DataSheetInterface $requestLogData, array $headers)
	{
		$validator = $this->validateRouteSwagger($routeModel);
		if (!$validator->isValid()){
			$response = $this->createSwaggerErrorResponse($headers, $validator->getErrors());
			$requestLogData = $this->logRequestFailed($requestLogData, new \InvalidArgumentException('Invalid swagger json.'), $response);
			return $response;
		}

		return null;
	}


    /**
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::getUrlRouteDefault()
     */
    public function getUrlRouteDefault(): string
    {
        return 'api/dataflow';
    }

    /**
     * @param string $flowAlias
     * @param ServerRequestInterface $request
     * @param DataSheetInterface $requestLogData
     * @return ResultInterface
     */
    protected function runFlow(string $flowAlias, ServerRequestInterface $request, DataSheetInterface $requestLogData) : ResultInterface
    {
        $task = new HttpTask($this->getWorkbench(), $this, $request);
        $task->setInputData($requestLogData);

        $actionSelector = new ActionSelector($this->getWorkbench(), RunETLFlow::class);
        /* @var $action \axenox\ETL\Actions\RunETLFlow */
        $action = ActionFactory::createFromPrototype($actionSelector, $this->getApp());
        $action->setMetaObject($requestLogData->getMetaObject());
        $action->setFlowAlias($flowAlias);
        $action->setInputFlowRunUid('flow_run');
        $result = $action->handle($task);
        return $result;
    }

    /**
     * @param string $routeUID
     * @param string $flowRunUID
     * @param ServerRequestInterface $request
     * @return DataSheetInterface
     */
    protected function logRequestReceived(ServerRequestInterface $request) : DataSheetInterface
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice_request');
        $ds->addRow([
            'status' => WebRequestStatusDataType::RECEIVED,
            'url' => $request->getUri()->__toString(),
            'url_path' => StringDataType::substringAfter($request->getUri()->getPath(), $this->getUrlRouteDefault() . '/', $request->getUri()->getPath()),
            'http_method' => $request->getMethod(),
            'http_headers' => JsonDataType::encodeJson($request->getHeaders()),
            'http_body' => $request->getBody()->__toString(),
            'http_content_type' => implode(';', $request->getHeader('Content-Type'))
        ]);
        $ds->dataCreate(false);
        return $ds;
    }

    /**
     * @param string $routeUID
     * @param string $flowRunUID
     * @param ServerRequestInterface $request
     * @return DataSheetInterface
     */
    protected function logRequestProcessing(DataSheetInterface $requestLogData, string $routeUID, string $flowRunUID) : DataSheetInterface
    {
        $ds = $requestLogData->extractSystemColumns();
        $ds->setCellValue('route', 0, $routeUID);
        $ds->setCellValue('status', 0, WebRequestStatusDataType::PROCESSING);
        $ds->setCellValue('flow_run', 0, $flowRunUID);
        $ds->dataUpdate(false);
        return $ds;
    }

    /**
     * @param string $requestLogUID
     * @param ExceptionInterface $e
     * @return DataSheetInterface
     */
    protected function logRequestFailed(DataSheetInterface $requestLogData, \Throwable $e, ResponseInterface $response = null) : DataSheetInterface
    {
        if (! ($e instanceof ExceptionInterface)) {
            $e = new InternalError($e->getMessage(), null, $e);
        }
        $this->getWorkbench()->getLogger()->logException($e);
        $ds = $requestLogData->extractSystemColumns();
        $ds->setCellValue('status', 0, WebRequestStatusDataType::ERROR);
        $ds->setCellValue('error_message', 0, $e->getMessage());
        $ds->setCellValue('error_logid', 0, $e->getId());
        $ds->setCellValue('http_response_code', 0, $response !== null ? $response->getStatusCode() : $e->getStatusCode());
        $ds->setCellValue('response_header', 0, json_encode($response->getHeaders()));
        $ds->setCellValue('response_body', 0, $response->getBody()->__toString());
        $ds->dataUpdate(false);
        return $ds;
    }

    /**
     * @param string $requestLogUID
     * @param string $output
     * @return DataSheetInterface
     */
    protected function logRequestDone(DataSheetInterface $requestLogData, string $output, ResponseInterface $response) : DataSheetInterface
    {
        $ds = $requestLogData->extractSystemColumns();
        $ds->setCellValue('status', 0, WebRequestStatusDataType::DONE);
        $ds->setCellValue('result_text', 0, $output);
        $ds->setCellValue('http_response_code', 0, $response->getStatusCode());
        $ds->setCellValue('response_header', 0, json_encode($response->getHeaders()));
        $ds->setCellValue('response_body', 0, $response->getBody()->__toString());
        $ds->dataUpdate(false);
        return $ds;
    }

    /**
     * @param string $route
     * @throws FacadeRoutingError
     * @return string[]
     */
    protected function getRouteData(ServerRequestInterface $request) : ?array
    {
        return $request->getAttribute(self::REQUEST_ATTRIBUTE_NAME_ROUTE, null);
    }

    /**
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::getMiddleware()
     */
    protected function getMiddleware() : array
    {
        $middleware = parent::getMiddleware();
        
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice_route');
        $ds->getColumns()->addMultiple([
            'UID',
            'flow',
            'flow__alias',
            'in_url',
            'type__schema_json',
            'type__default_response_path',
            'swagger_json',
            'config_uxon'
        ]);
        $ds->dataRead();
        $middleware[] = new RouteConfigLoader(
            $this,
            $ds,
            'in_url',
            'config_uxon',
            self::REQUEST_ATTRIBUTE_NAME_ROUTE
        );

        return $middleware;
    }

    /** Validating swagger json against the corresonding schema json from the route type.
     * 
     * @param array $routeModel
     * @return Validator
     */
    private function validateRouteSwagger(array $routeModel) : Validator
    {
    	$validator = (new Validator);
    	$validator->validate(json_decode($routeModel['swagger_json']), json_decode($routeModel['type__schema_json']));
    	return $validator;
    }


    /**
     * @param array $headers
     * @param array $errors
     */
    private function createSwaggerErrorResponse(array $headers, array $validatorErrors) : ResponseInterface
    {
    	$headers['Content-Type'] = 'application/json';
    	$errors = ['Invalid Swagger' => []];
    	foreach ($validatorErrors as $error){
    		array_push(
    			$errors['Invalid Swagger'],
    			array('source' => $error['property'], 'message' => $error['message']));
    	}

    	return new Response(400, $headers, json_encode($errors));
    }
    
    /**
     * @param string $routePath
     * @param string $methodType
     * @param string $jsonPath
     * @param string $swaggerJson
     * @return string
     */
    private function createEmptyRequestBodyFromSwaggerJson(
    	string $routePath,
    	string $methodType,
    	string $jsonPath,
    	string $swaggerJson) : string
    	{
    	    require_once '..' . DIRECTORY_SEPARATOR 
        	    . '..' . DIRECTORY_SEPARATOR
        	    . 'axenox' . DIRECTORY_SEPARATOR
        	    . 'etl' . DIRECTORY_SEPARATOR
        	    . 'Common' . DIRECTORY_SEPARATOR
        	    . 'JSONPath' . DIRECTORY_SEPARATOR
        	    . 'JSONPathLexer.php';
    	    $jsonPath = str_replace('[#routePath#]', $routePath, $jsonPath);
    		$jsonPath = str_replace('[#methodType#]', $methodType, $jsonPath);
    		$body = (new JSONPath(json_decode($swaggerJson, false)))->find($jsonPath)->getData();
    		return json_encode($body);
    }

    /**
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::createResponseFromError()
     */
    protected function createResponseFromError(\Throwable $exception, ServerRequestInterface $request = null) : ResponseInterface
    {
    	$code = $exception->getStatusCode();
    	$headers = $this->buildHeadersCommon();
    	
    	if ($this->getWorkbench()->getSecurity()->getAuthenticatedToken()->isAnonymous()) {
    		return new Response($code, $headers);
    	}

    	$headers['Content-Type'] = 'application/json';
    	$errorData = json_encode([
	        'Error' => [
    		    'Message' => $exception->getMessage(),
    			'Log-Id' => $exception->getId()
    		]
        ]);
    	
    	return new Response($code, $headers, $errorData);
    }
    
    /**
     * @param DataSheetInterface $requestLogData
     */
    private function reloadRequestData(DataSheetInterface $requestLogData)
     {
     	$requestLogData->getFilters()->addConditionFromColumnValues($requestLogData->getUidColumn());
     	$requestLogData->getColumns()->addFromExpression('response_body');
     	$requestLogData->dataRead();
     }

    protected function buildHtmlSwaggerUI(string $openapiUrl) : string
    {
        $siteRoot = '../../../';
        $swaggerUI = $siteRoot . 'vendor/npm-asset/swagger-ui-dist';
        
        return <<<HTML
        <!-- HTML for static distribution bundle build -->
        <!DOCTYPE html>
        <html lang='en'>
          <head>
            <meta charset='UTF-8'>
            <title>Swagger UI</title>
            <link rel='stylesheet' type='text/css' href='{$swaggerUI}/swagger-ui.css' />
            <link rel='stylesheet' type='text/css' href='{$swaggerUI}/index.css' />
            <link rel='icon' type='image/png' href='{$swaggerUI}/favicon-32x32.png' sizes='32x32' />
            <link rel='icon' type='image/png' href='{$swaggerUI}/favicon-16x16.png' sizes='16x16' />
          </head>

          <body>
            <div id='swagger-ui'></div>
            <script src='{$swaggerUI}/swagger-ui-bundle.js' charset='UTF-8'> </script>
            <script src='{$swaggerUI}/swagger-ui-standalone-preset.js' charset='UTF-8'> </script>
            <script>
                window.onload = function() {
                //<editor-fold desc='Changeable Configuration Block'>

                // the following lines will be replaced by docker/configurator, when it runs in a docker-container
                window.ui = SwaggerUIBundle({
                    url: '{$openapiUrl}',
                    dom_id: '#swagger-ui',
                    deepLinking: true,
     				defaultModelsExpandDepth: 4,
     				showExtensions: true,
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIStandalonePreset
                    ],
                    plugins: [
                        SwaggerUIBundle.plugins.DownloadUrl
                    ],
                    layout: 'StandaloneLayout'
                    });

                    //</editor-fold>
                };
            </script>
          </body>
        </html>
HTML;
    }
    
    /**
     * @param MetaObjectInterface $metaobject
     * @param array $attributeAliasesToAdd
     */
    protected function transformIntoJsonSchema(MetaObjectInterface $metaobject, array $attributeAliasesToAdd) : array
    {
		$objectName = $metaobject->getAliasWithNamespace();
		$jsonSchema = [$objectName => ['type' => 'object', 'properties' => []]];

		foreach ($metaobject->getAttributes() as $attribute) {
			$dataType = $attribute->getDataType();
			switch (true) {
				case $attribute->isRelation():
					$relatedObjectAlias =  $attribute->getRelation()->getRightObject()->getAliasWithNamespace();
					if (in_array($relatedObjectAlias, $attributeAliasesToAdd)){
						$schema = ['$ref' => '#/components/schemas/Metamodel Informationen/properties/' . $relatedObjectAlias];
						$jsonSchema[$objectName]['properties'][$attribute->getAlias()] = $schema;
						continue 2;
					}
				case $dataType instanceof IntegerDataType:
					$schema = ['type' => 'integer'];
					break;
				case $dataType instanceof NumberDataType:
					$schema = ['type' => 'number'];
					break;
				case $dataType instanceof BooleanDataType:
					$schema = ['type' => 'boolean'];
					break;
				case $dataType instanceof ArrayDataType:
					$schema = ['type' => 'array'];
					break;
				case $dataType instanceof EnumDataTypeInterface:
					$schema = ['type' => 'string', 'enum' => $dataType->getValues()];
					break;
				case $dataType instanceof DateTimeDataType:
					$schema = ['type' => 'string', 'format' => 'datetime'];
					break;
				case $dataType instanceof DateDataType:
					$schema = ['type' => 'string', 'format' => 'date'];
					break;
				case $dataType instanceof BinaryDataType:
					if ($dataType->getEncoding() == 'base64') {

						$schema = ['type' => 'string', 'format' => 'byte'];
					} else {
						$schema = ['type' => 'string', 'format' => 'binary'];
					}
					break;
				case $dataType instanceof StringDataType:
					$schema = ['type' => 'string'];
					break;
				default:
					throw new InvalidArgumentException('Datatype: ' . $dataType . ' not recognized.');
			}
		
			if ($attribute->getHint() !== $attribute->getName()){			
				$schema['description'] = $attribute->getHint();
			}
			
			$jsonSchema[$objectName]['properties'][$attribute->getAlias()] = $schema;
		}

		return $jsonSchema;
    }
}
