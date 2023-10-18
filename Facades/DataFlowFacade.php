<?php
namespace axenox\ETL\Facades;

use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Message\ResponseInterface;
use function GuzzleHttp\Psr7\stream_for;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Request;
use exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Facades\AbstractHttpFacade\Middleware\AuthenticationMiddleware;
use exface\Core\Exceptions\Facades\FacadeRoutingError;
use axenox\Proxy\Facades\RequestHandlers\DefaultProxy;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use exface\Core\DataTypes\JsonDataType;
use exface\Core\Interfaces\Exceptions\ExceptionInterface;
use exface\Core\DataTypes\UUIDDataType;
use exface\Core\Interfaces\Tasks\ResultInterface;
use exface\Core\Interfaces\Tasks\TaskInterface;
use exface\Core\CommonLogic\Tasks\HttpTask;
use axenox\ETL\Actions\RunETLFlow;
use exface\Core\Factories\ActionFactory;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\CommonLogic\Selectors\ActionSelector;
use exface\Core\Exceptions\InternalError;
use axenox\ETL\DataTypes\WebRequestStatusDataType;

/**
 * 
 * 
 * @author Andrej Kabachnik
 *
 */
class DataFlowFacade extends AbstractHttpFacade
{    
    private $routesData = null;
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::createResponse()
     */
    protected function createResponse(ServerRequestInterface $request) : ResponseInterface
    {
        $requestLogData = $this->logRequestReceived($request);
        
        try {
            $path = $request->getUri()->getPath();
            $path = StringDataType::substringAfter($path, $this->getUrlRouteDefault() . '/', '');
            $routeModel = $this->getRouteData($path);
            
            $routeUID = $routeModel['UID'];
            $flowAlias = $routeModel['flow__alias'];
            $flowRunUID = RunETLFlow::generateFlowRunUid();
            
            $requestLogData = $this->logRequestProcessing($requestLogData, $routeUID, $flowRunUID);
            
            $flowResult = $this->runFlow($flowAlias, $request, $requestLogData);
            $flowOutput = $flowResult->getMessage();
        } catch (\Throwable $e) {
            $response = $this->createResponseFromError($e, $request);
            $requestLogData = $this->logRequestFailed($requestLogData, $e, $response);
            return $response;
        }
        
        $headers = $this->buildHeadersCommon();
        $headers['Content-Type'] = 'application/json';
        $response = new Response(200, $headers, 'Data flow successfull');
        
        $requestLogData = $this->logRequestDone($requestLogData, $flowOutput, $response);
        return $response;
    }

    /**
     *
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::getUrlRouteDefault()
     */
    public function getUrlRouteDefault(): string
    {
        return 'api/dataflow';
    }
    
    /**
     * 
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
     * 
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
     * 
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
     * 
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
        $ds->setCellValue('status', 0,WebRequestStatusDataType::ERROR);
        $ds->setCellValue('error_message', 0, $e->getMessage());
        $ds->setCellValue('error_logid', 0, $e->getId());
        $ds->setCellValue('http_response_code', 0, $response !== null ? $response->getStatusCode() : $e->getStatusCode());
        $ds->dataUpdate(false);
        return $ds;
    }
    
    /**
     * 
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
        $ds->dataUpdate(false);
        return $ds;
    }
    
    /**
     * 
     * @param string $route
     * @throws FacadeRoutingError
     * @return string[]
     */
    protected function getRouteData(string $route) : array
    {
        if ($this->routesData === null) {
            $ds = DataSheetFactory::createFromObjectIdOrAlias($this->getWorkbench(), 'axenox.ETL.webservice_route');
            $ds->getColumns()->addMultiple([
                'UID',
                'flow',
                'flow__alias',
                'in_url'
            ]);
            $ds->dataRead();
            $this->routesData = $ds;
        }
        
        foreach ($this->routesData->getRows() as $row) {
            if ($row['in_url'] && StringDataType::startsWith($route, $row['in_url'])) {
                return $row;
            }
        }
        
        throw new FacadeRoutingError('No route configuration found for "' . $route . '"');
    }
    
    /**
     * 
     * {@inheritDoc}
     * @see \exface\Core\Facades\AbstractHttpFacade\AbstractHttpFacade::getMiddleware()
     */
    protected function getMiddleware() : array
    {
        $middleware = parent::getMiddleware();
        $middleware[] = new AuthenticationMiddleware(
            $this,
            [
                [AuthenticationMiddleware::class, 'extractBasicHttpAuthToken']
            ]
        );
        
        return $middleware;
    }
}