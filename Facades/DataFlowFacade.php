<?php
namespace axenox\ETL\Facades;

use axenox\ETL\Facades\Middleware\RequestLoggingMiddleware;
use exface\Core\Exceptions\InvalidArgumentException;
use Flow\JSONPath\JSONPathException;
use GuzzleHttp\Psr7\Response;
use Intervention\Image\Exception\NotFoundException;
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
use exface\Core\Exceptions\DataTypes\JsonSchemaValidationError;
use exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade;
use exface\Core\Facades\AbstractHttpFacade\Middleware\RouteConfigLoader;
use exface\Core\Factories\ActionFactory;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\Interfaces\Exceptions\ExceptionInterface;
use exface\Core\Interfaces\Tasks\ResultInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;
use axenox\ETL\Facades\Middleware\OpenApiValidationMiddleware;
use axenox\ETL\Facades\Middleware\OpenApiMiddleware;
use axenox\ETL\Facades\Middleware\SwaggerUiMiddleware;
use Flow\JSONPath\JSONPath;
use axenox\ETL\Facades\Middleware\RouteAuthenticationMiddleware;


/**
 * 
 * 
 * @author Andrej Kabachnik
 * 
 */
class DataFlowFacade extends AbstractHttpFacade implements OpenApiFacadeInterface
{
    // TODO: move all OpenApiFacadeInterface methods into a separate OpenApiWebserviceType class

	const REQUEST_ATTRIBUTE_NAME_ROUTE = 'route';
    const REQUEST_ATTRIBUTE_FORMATTED_RESPONSE = 'FORMATTED_RESPONSE';

	private $openApiCache = [];
    private $logData = null;
    private RequestLoggingMiddleware $loggingMiddleware;

    /**
	 * {@inheritDoc}
	 * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::createResponse()
	 */
	protected function createResponse(ServerRequestInterface $request): ResponseInterface
	{    
	    $headers = $this->buildHeadersCommon();
        $response = null;

		try {
            $path = $this->getRoutePath($request);
            $routePath = rtrim(strstr($path, '/'), '/');
			$routeModel = $this->getRouteData($request);

			// process flow
			$routeUID = $routeModel['UID'];
			$flowAlias = $this->getFlowAlias($routeUID, $routePath);
			$flowRunUID = RunETLFlow::generateFlowRunUid();
            $this->loggingMiddleware->logRequestProcessing($request, $routeUID, $flowRunUID);
			$flowResult = $this->runFlow($flowAlias, $request); // flow data update
			$flowOutput = $flowResult->getMessage();
            $requestWithBody = $this->loadRequestDataWithBody($request);
				
			if ($requestWithBody->countRows() == 1) {
				$body = $this->createRequestResponseBody($requestWithBody, $request, $headers, $routeModel, $routePath);
				$response = new Response(200, $headers, $body);
			}
			
		} catch (\Throwable $e) {
            $this->loadRequestDataWithBody($request);
			if (!$e instanceof ExceptionInterface) {
				$e = new InternalError($e->getMessage(), null, $e);
			}

            return $this->createResponseFromError($e, $request);
		}

		if ($response === null) {
			$response = new Response(200, $headers, 'Dataflow successfull.');
		}

        $this->loggingMiddleware->logRequestDone($request, $flowOutput, $response);
		return $response;
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
     * @throws \Throwable
     */
	protected function runFlow(string $flowAlias, ServerRequestInterface $request): ResultInterface
	{
        $taskData = $this->loggingMiddleware->getTaskData($request);
		$task = new HttpTask($this->getWorkbench(), $this, $request);
		$task->setInputData($taskData);

		$actionSelector = new ActionSelector($this->getWorkbench(), RunETLFlow::class);
		/* @var $action \axenox\ETL\Actions\RunETLFlow */
		$action = ActionFactory::createFromPrototype($actionSelector, $this->getApp());
		$action->setMetaObject($taskData->getMetaObject());
		$action->setFlowAlias($flowAlias);
		$action->setInputFlowRunUid('flow_run');

        $routeData = $request->getAttribute(self::REQUEST_ATTRIBUTE_NAME_ROUTE);
        $action->setOpenApiJson($routeData['swagger_json']);

		$result = $action->handle($task);
		return $result;
	}

    /**
     *
     * /**
     * @param DataSheetInterface $requestLogData
     * @param ServerRequestInterface $request
     * @param array $headers
     * @param array $routeModel
     * @param string $routePath
     * @return string|null
     * @throws JSONPathException
     */
	private function createRequestResponseBody(
		DataSheetInterface $requestLogData,
		ServerRequestInterface $request,
		array &$headers,
		array $routeModel,
		string $routePath) : ?string
	{
        // body already created in step
        $responseHeader = $requestLogData->getRow()['response_header'];
        if ($responseHeader  !== null) {
            $headers['Content-Type'] = $responseHeader;
            return $requestLogData->getRow()['response_body'];
        }

		$flowResponse = json_decode($requestLogData->getRow()['response_body'], true);
        $responseModel = null;
		
		// load response model from swagger
		$methodType = strtolower($request->getMethod());
		$jsonPath = $routeModel['type__default_response_path'];
		if ($jsonPath !== null) {
			$responseModel = $this->readDataFromSwaggerJson(
				$routePath,
				$methodType,
				$jsonPath,
				$routeModel['swagger_json']
		    );
		}

		if ($responseModel === null && empty($responseModel)) {
            return null;
		}

        $headers['Content-Type'] = 'application/json';
		// merge flow response into empty model
        if (empty($flowResponse) === false){
            $body = array_merge($responseModel, $flowResponse);
        } else {
            $body = $responseModel;
        }

		return json_encode($body);
	}

    /**
     * @param ServerRequestInterface $request
     * @return string[]|null
     */
	protected function getRouteData(
		ServerRequestInterface $request) : ?array
	{
		return $request->getAttribute(self::REQUEST_ATTRIBUTE_NAME_ROUTE, null);
	}

    protected function getFlowAlias(string $routeUid, string $routePath) : string
    {
        $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice_flow');
        $ds->getColumns()->addMultiple(['webservice', 'flow__alias', 'route']);
        $ds->getFilters()->addConditionFromString('webservice', $routeUid);
        $ds->dataRead();

        $alias = null;
        $rows = $ds->getRows();
        foreach ($rows as $row){
            if (str_contains($row['route'], ltrim($routePath,'/'))) {
                $alias = $row['flow__alias'];
                return $alias;
            }
        }

        if ($alias === null && count($rows) === 1){
            return $rows[0]['flow__alias'];
        } else {
            throw new NotFoundException('Cannot find flow to webservice `' . $routeUid . '`');
        }
    }

	/**
	 * {@inheritDoc}
	 * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::getMiddleware()
	 */
	protected function getMiddleware(): array
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
	    
		$ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice');
		$ds->getColumns()->addMultiple(
			['UID', 'local_url', 'type__schema_json', 'type__default_response_path', 'swagger_json', 'config_uxon']);
		$ds->dataRead();

        $excludePattern = ['/.*swaggerui$/', '/.*openapi\\.json$/'];
        $loggingMiddleware = new RequestLoggingMiddleware($this, $excludePattern);
        $this->loggingMiddleware = $loggingMiddleware;

		$middleware = parent::getMiddleware();
		$middleware[] = new RouteConfigLoader($this, $ds, 'local_url', 'config_uxon', self::REQUEST_ATTRIBUTE_NAME_ROUTE);
		$middleware[] = new RouteAuthenticationMiddleware($this, [], true);
		$middleware[] = $loggingMiddleware;
        $middleware[] = new OpenApiValidationMiddleware($this, $excludePattern,
		    // TODO allow to customize the URL parameter for verbose output in service UXON
		    true
	    );
		$middleware[] = new OpenApiMiddleware($this, $this->buildHeadersCommon(), '/.*openapi\\.json$/');
		$middleware[] = new SwaggerUiMiddleware($this, $this->buildHeadersCommon(), '/.*swaggerui$/', 'openapi.json');
		
		return $middleware;
	}

	/**
	 * {@inheritDoc}
	 * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::createResponseFromError()
	 */
	protected function createResponseFromError(\Throwable $exception, ServerRequestInterface $request = null): ResponseInterface
	{
		$code = $exception->getStatusCode();
		$headers = $this->buildHeadersCommon();

        /*
		if ($this->getWorkbench()
			->getSecurity()
			->getAuthenticatedToken()
			->isAnonymous()) {
            $response = new Response($code, $headers);
            // Don't log anonymous requests to avoid flooding the request log
            return $response;
		}
        */

        $headers['Content-Type'] = 'application/json';
        if ($exception instanceof JsonSchemaValidationError) {
            $response = new Response(400, $headers, json_encode($exception->getFormattedErrors()));
            $this->loggingMiddleware->logRequestFailed($request, $exception, $response);
            return $response;
		}

        $errorData = json_encode(['Error' => [
			'Message' => $exception->getMessage(), 
			'Log-Id' => $exception->getId()]
		]);

		$response = new Response($code, $headers, $errorData);

        $this->loggingMiddleware->logRequestFailed($request, $exception, $response);
        return $response;
	}

	/**
	 * @param DataSheetInterface $requestLogData
	 */
	private function loadRequestDataWithBody(ServerRequestInterface $request) : DataSheetInterface
	{
        $requestLogData = $this->loggingMiddleware->getLogData($request);
		$requestLogData->getFilters()->addConditionFromColumnValues($requestLogData->getUidColumn());
		$requestLogData->getColumns()->addFromExpression('response_body');
        $requestLogData->getColumns()->addFromExpression('response_header');
		$requestLogData->dataRead();
        return $requestLogData;
	}

	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::buildHeadersCommon()
	 */
	protected function buildHeadersCommon(): array
	{
		$facadeHeaders = array_filter($this->getConfig()
			->getOption('FACADE.HEADERS.COMMON')
			->toArray());
		$commonHeaders = parent::buildHeadersCommon();
		return array_merge($commonHeaders, $facadeHeaders);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::getOpenApiJson()
	 */
	public function getOpenApiJson(ServerRequestInterface $request): ?array
	{
		$path = $request->getUri()->getPath();
		if (array_key_exists($path, $this->openApiCache)) {
			return $this->openApiCache[$path];
		}
		$routeData = $request->getAttribute(self::REQUEST_ATTRIBUTE_NAME_ROUTE);
		if (empty($routeData)) {
			throw new FacadeRoutingError('No route data found in request!');
		}
		$json = $routeData['swagger_json'];
		if ($json === null || $json === '') {
			return null;
		}
		
		JsonDataType::validateJsonSchema($json, $routeData['type__schema_json']);		
		$jsonArray = json_decode($json, true);
		$jsonArray = $this->prependLocalServerPaths($path, $jsonArray);
		$this->openApiCache[$path] = $jsonArray;
		return $jsonArray;
	}

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::getRequestBodySchemaForCurrentRoute()
     */
    public function getRequestBodySchemaForCurrentRoute(ServerRequestInterface $request): object
    {
        $jsonPath = '$.paths.[#routePath#].[#methodType#].requestBody';
        $contentType = $request->getHeader('Content-Type')[0];
        return $this->getJsonSchemaFromOpenApiByRef($request, $jsonPath, $contentType);
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::getResponseBodySchemaForCurrentRoute()
     */
    public function getResponseBodySchemaForCurrentRoute(ServerRequestInterface $request, int $responseCode): object
    {
        $jsonPath = '$.paths.[#routePath#].[#methodType#].responses.' . $responseCode;
        $contentType = $request->getHeader('accept')[0];
        return $this->getJsonSchemaFromOpenApiByRef($request, $jsonPath, $contentType);
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::getJsonSchemaFromOpenApiByRef()
     */
    public function getJsonSchemaFromOpenApiByRef(ServerRequestInterface $request, string $jsonPath, string $contentType): object
    {
        $openApiSchema = $this->getOpenApiJson($request);
        $jsonPath = $this->findSchemaPathInOpenApiJson($request, $jsonPath, $contentType);
        $schema = $this->findJsonDataByRef($openApiSchema, $jsonPath);
        if ($schema === null) {
            throw new InvalidArgumentException('Could not find schema with given json path in OpenApi.'
            . ' Json path: ' . $jsonPath);
        }

        $schema = is_array($schema) ? json_decode(json_encode($schema)) : $schema;
        return $this->convertNullableToNullType($schema);
    }

    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::convertNullableToNullType()
     */
    public function convertNullableToNullType(object $schema) : object
    {
        if ($schema instanceof \StdClass === false) {
            throw new InvalidArgumentException('');
        }

        foreach ($schema as $objectPart) {
            if (is_object($objectPart) === false) {
                continue;
            }

            if (property_exists($objectPart, 'nullable') && $objectPart->nullable === true) {
                $type = $objectPart->type;
                $objectPart->type = [ $type,  'null'];
            } else {
                $this->convertNullableToNullType($objectPart);
            }
        }

        return $schema;
    }


    /**
     *
     * {@inheritDoc}
     * @see \axenox\ETL\Interfaces\OpenApiFacadeInterface::findSchemaPathInOpenApiJson()
     */
    public function findSchemaPathInOpenApiJson(ServerRequestInterface $request, string $jsonPath, string $contentType): string
    {
        $path = $this->getRoutePath($request);
        $routePath = rtrim(strstr($path, '/'), '/');
        $methodType = strtolower($request->getMethod());

        $jsonPath .= '.content.[#ContentType#].schema';
        return str_replace(
            ['[#routePath#]', '[#methodType#]', '[#ContentType#]'],
            [$routePath, $methodType, $contentType],
            $jsonPath);
    }

    /**
     * @param array|null $openApiSchema
     * @param string $jsonPath
     * @return mixed|null
     * @throws \Flow\JSONPath\JSONPathException
     */
    public function findJsonDataByRef(?array $openApiSchema, string $jsonPath): mixed
    {
        $jsonPathFinder = new JSONPath($openApiSchema);
        $refSchema = $jsonPathFinder->find($jsonPath)->getData()[0] ?? null;
        if ($refSchema != null) {
            $refSchema = str_replace('#', '$', $refSchema['$ref']);
            $refSchema = str_replace('/', '.', $refSchema);
            return $jsonPathFinder->find($refSchema)->getData()[0] ?? null;
        }

        return null;
    }
	
	/**
	 * @param string $path
	 * @param array $swaggerArray
	 * @return array
	 */
	private function prependLocalServerPaths(string $path, array $swaggerArray): array
	{
		$basePath = $this->getUrlRouteDefault();
		$routePath = StringDataType::substringAfter($path, $basePath, $path);
		$webserviceBase = StringDataType::substringBefore($routePath, '/', '', true, true) . '/';
		$basePath .= '/' . ltrim($webserviceBase, "/");
		foreach ($this->getWorkbench()->getConfig()->getOption('SERVER.BASE_URLS') as $baseUrl) {
            // prepend entry to array
            array_unshift($swaggerArray['servers'], ['url' => $baseUrl . $basePath]);
		}
			
		return $swaggerArray;
	}

    /**
     * Selects data from a swaggerJson with the given json path.
     * Route path and method type are used to replace placeholders within the path.
     *
     * @param string $routePath
     * @param string $methodType
     * @param string $jsonPath
     * @param string $swaggerJson
     * @return array|null
     * @throws JSONPathException
     */
	public function readDataFromSwaggerJson(
		string $routePath,
		string $methodType,
		string $jsonPath,
		string $swaggerJson): ?array
    {
			$jsonPath = str_replace('[#routePath#]', $routePath, $jsonPath);
			$jsonPath = str_replace('[#methodType#]', $methodType, $jsonPath);
			$data = (new JSONPath(json_decode($swaggerJson, false)))->find($jsonPath)->getData()[0] ?? null;
			return is_object($data) ? get_object_vars($data) : $data;
	}

    /**
     * @param ServerRequestInterface $request
     * @return string
     */
    public function getRoutePath(ServerRequestInterface $request): string
    {
        $path = $request->getUri()->getPath();
        return StringDataType::substringAfter($path, $this->getUrlRouteDefault() . '/', '');
    }
}
