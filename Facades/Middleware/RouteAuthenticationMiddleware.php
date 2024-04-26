<?php
namespace axenox\ETL\Facades\Middleware;

use exface\Core\Facades\AbstractHttpFacade\Middleware\AuthenticationMiddleware;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Server\RequestHandlerInterface;
use axenox\ETL\Facades\DataFlowFacade;
use exface\Core\CommonLogic\UxonObject;

/**
 * 
 * 
 * @author Andrej Kabachnik
 *
 */
class RouteAuthenticationMiddleware extends AuthenticationMiddleware
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $routeData = $request->getAttribute(DataFlowFacade::REQUEST_ATTRIBUTE_NAME_ROUTE);
        if ($routeData === null) {
            return $handler->handle($request);
        }
        $routeConfig = $routeData['config_uxon'];
        if ($routeConfig === null || $routeConfig === '') {
            return $handler->handle($request);
        }
        $routeUxon = UxonObject::fromJson($routeConfig);
        if ($routeUxon->hasProperty('authentication')) {
            $this->importUxonObject($routeUxon->getProperty('authentication'));
        }
        return parent::process($request, $handler);
    }
}
