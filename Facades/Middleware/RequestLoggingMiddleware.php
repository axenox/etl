<?php
namespace axenox\ETL\Facades\Middleware;

use axenox\ETL\DataTypes\WebRequestStatusDataType;
use exface\Core\DataTypes\JsonDataType;
use exface\Core\DataTypes\StringDataType;
use exface\Core\Factories\DataSheetFactory;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;

/**
 * This middleware handels all logging with a webservice request.
 * 
 * @author miriam.seitz
 *
 */
final class RequestLoggingMiddleware implements MiddlewareInterface
{
    private $facade = null;
    private $excludePatterns = [];
    private $finished = false;

    private DataSheetInterface|null $logData = null;

    private DataSheetInterface|null $taskData = null;

    /**
     * @param OpenApiFacadeInterface $facade
     * @param array $excludePatterns
     */
    public function __construct(OpenApiFacadeInterface $facade,  array $excludePatterns = [])
    {
        $this->excludePatterns = $excludePatterns;
        $this->facade = $facade;
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

        $this->logRequestReceived($request);
        $response = $handler->handle($request);
        if ($this->finished === false) {
            if ($response->getStatusCode() < 300) {
                $this->logRequestDone($request, '', $response);
            } else {
                $this->logRequestFailed($request, null, $response);
            }
        }
        return $response;
    }

    /**
     * @param ServerRequestInterface $request
     * @return void
     */
    public function logRequestReceived(
        ServerRequestInterface $request): void
    {
        $logData = DataSheetFactory::createFromObjectIdOrAlias(
            $this->facade->getWorkbench(),
            'axenox.ETL.webservice_request');
        $logData->addRow([
            'status' => WebRequestStatusDataType::RECEIVED,
            'url' => $request->getUri()->__toString(),
            'url_path' => StringDataType::substringAfter(
                $request->getUri()->getPath(),
                $this->facade->getUrlRouteDefault() . '/',
                $request->getUri()->getPath()),
            'http_method' => $request->getMethod(),
            'http_headers' => JsonDataType::encodeJson($request->getHeaders()),
            'http_body' => $request->getBody()->__toString(),
            'http_content_type' => implode(';', $request->getHeader('Content-Type'))]);

        $logData->dataCreate(false);
        $this->logData = $logData;
    }

    /**
     * @param ServerRequestInterface $request
     * @param string $routeUID
     * @param string $flowRunUID
     * @return void
     */
    public function logRequestProcessing(
        ServerRequestInterface $request,
        string $routeUID,
        string $flowRunUID): void
    {
        // create request log if missing
        if ($this->logData === null) {
            $this->logRequestReceived($request);
        }

        $taskData = $this->logData->extractSystemColumns();
        $taskData->setCellValue('route', 0, $routeUID);
        $taskData->setCellValue('status', 0, WebRequestStatusDataType::PROCESSING);
        $taskData->setCellValue('flow_run', 0, $flowRunUID);
        $taskData->dataUpdate(false);
        $this->taskData = $taskData;
    }

    /**
     * @param ServerRequestInterface $request
     * @param \Throwable|NULL $e
     * @param ResponseInterface|null $response
     * @return void
     */
    public function logRequestFailed(
        ServerRequestInterface $request,
        \Throwable $e = null,
        ResponseInterface $response = null): void
    {
        // do not log errors in request log prior to a valid request
        if ($this->logData === null) {
            return;
        }

        $logData = $this->logData->extractSystemColumns();
        $logData->setCellValue('status', 0, WebRequestStatusDataType::ERROR);
        if ($e !== null) {
            $logData->setCellValue('error_message', 0, $e->getMessage());
            $logData->setCellValue('error_logid', 0, $e->getId());
            $logData->setCellValue('http_response_code', 0, $e->getStatusCode());
        }
        if ($response !== null) {
            $logData->setCellValue('http_response_code', 0, $response->getStatusCode());
            $logData->setCellValue('response_header', 0, json_encode($response->getHeaders()));
            $logData->setCellValue('response_body', 0, $response->getBody()->__toString());
        }
        $logData->dataUpdate(false);
        $this->logData->merge($logData);
        $this->finished = true;
    }

    /**
     * @param ServerRequestInterface $request
     * @param string $output
     * @param ResponseInterface $response
     * @return void
     */
    public function logRequestDone(
        ServerRequestInterface $request,
        string $output,
        ResponseInterface $response): void
    {
        $logData = $this->logData->extractSystemColumns();
        $logData->setCellValue('status', 0, WebRequestStatusDataType::DONE);
        $logData->setCellValue('result_text', 0, $output);
        $logData->setCellValue('http_response_code', 0, $response->getStatusCode());
        $logData->setCellValue('response_header', 0, json_encode($response->getHeaders()));
        $logData->setCellValue('response_body', 0, $response->getBody()->__toString());
        $logData->dataUpdate(false);
        $this->logData->merge($logData);
        $this->finished = true;
    }

    /**
     * @param ServerRequestInterface $request
     * @return DataSheetInterface|null
     */
    public function getLogData(ServerRequestInterface $request) : ?DataSheetInterface
    {
        return $this->logData;
    }

    /**
     * @param ServerRequestInterface $request
     * @return DataSheetInterface|null
     */
    public function getTaskData(ServerRequestInterface $request) : ?DataSheetInterface
    {
        return $this->taskData;
    }
}
