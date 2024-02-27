<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\Facades\FacadeInterface;
use Psr\Http\Message\ServerRequestInterface;

interface OpenApiFacadeInterface extends FacadeInterface
{
    /**
     * 
     * @param ServerRequestInterface $request
     * @return array|NULL
     */
    public function getOpenApiJson(ServerRequestInterface $request) : ?array;
}