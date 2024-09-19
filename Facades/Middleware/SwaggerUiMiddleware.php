<?php
namespace axenox\ETL\Facades\Middleware;

use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;
use exface\Core\Interfaces\WorkbenchInterface;
use GuzzleHttp\Psr7\Response;

/**
 * This middleware creates a SwaggerUi html as a test environment.
 * 
 * @author miriam.seitz
 *
 */
final class SwaggerUiMiddleware implements MiddlewareInterface
{
    private $facade = null;
    private $routePattern;
    private $openApiRouteName;
    
    public function __construct(OpenApiFacadeInterface $facade, string $routePattern, string $openApiRouteName)
    {
        $this->facade = $facade;
        $this->routePattern = $routePattern;
        $this->openApiRouteName = $openApiRouteName;
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
        
        $content = $this->buildHtmlSwaggerUI($this->openApiRouteName);
        $headers = array_merge($this->facade->buildHeadersCommon(), ['Content-Type' => 'text/html']);
        return new Response(200, $headers, $content);
    }
    
    /**
     * Create a HTML that initiates the SwaggerUi dist with the given openapi url.
     * 
     * @param string $openapiUrl
     * @return string HTML
     */
    protected function buildHtmlSwaggerUI(string $openapiUrl): string
    	{
            $siteRoot = $this->facade->getWorkbench()->getUrl();
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
    
    
    protected function getWorkbench() : WorkbenchInterface
    {
        return $this->facade->getWorkbench();
    }
}
