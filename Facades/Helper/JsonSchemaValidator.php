<?php
namespace axenox\ETL\Facades\Helper;

use JsonSchema\Validator;
use Psr\Http\Message\ResponseInterface;
use GuzzleHttp\Psr7\Response;
use exface\Core\Interfaces\DataSheets\DataSheetInterface;
use axenox\ETL\Interfaces\OpenApiFacadeInterface;

/**
 * A Validator to validate a json against a json schema.
 * 
 * @author miriam.seitz
 *
 */
class JsonSchemaValidator
{
	private $facade;
	
	public function __construct(OpenApiFacadeInterface $facade)
	{
		$this->facade = $facade;
	}
	
	/** 
	 * Validating swagger json against the corresonding schema json from the route type.
	 *
	 * @param array $routeModel
	 * @return Validator
	 */
	private function validateRouteSwagger(array $routeModel): Validator
		{
			$validator = (new Validator());
			$validator->validate(json_decode($routeModel['swagger_json']), json_decode($routeModel['type__schema_json']));
			return $validator;
	}
	
	
	/**
	 * Creates an HTTP response with the validation response if anything was invalid. Otherwise returns null.
	 * 
	 * @param array $routeModel
	 * @param DataSheetInterface $requestLogData
	 * @param array $headers
	 */
	public function getSwaggerValidatorResponse(array $routeModel, DataSheetInterface $requestLogData, array $headers)
	{
		$validator = $this->validateRouteSwagger($routeModel);
		if (!$validator->isValid()) {
			$response = $this->createSwaggerErrorResponse($headers, $validator->getErrors());
			$requestLogData = $this->facade->logRequestFailed(
				$requestLogData,
				new \InvalidArgumentException('Invalid swagger json.'),
				$response);
			
			return $response;
		}
		
		return null;
	}
	
	/**
	 * @param array $headers
	 * @param array $validatorErrors
	 */
	private function createSwaggerErrorResponse(array $headers, array $validatorErrors): ResponseInterface
		{
			$headers['Content-Type'] = 'application/json';
			$errors = ['Invalid Swagger' => []];
			foreach ($validatorErrors as $error) {
				array_push($errors['Invalid Swagger'], array('source' => $error['property'], 'message' => $error['message']));
			}
			
			return new Response(400, $headers, json_encode($errors));
	}
}