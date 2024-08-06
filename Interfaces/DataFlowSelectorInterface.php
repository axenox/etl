<?php
namespace axenox\ETL\Interfaces;

use exface\Core\Interfaces\Selectors\AliasSelectorInterface;
use exface\Core\Interfaces\Selectors\UidSelectorInterface;
use exface\Core\Interfaces\Selectors\VersionSelectorInterface;

interface DataFlowSelectorInterface extends AliasSelectorInterface, VersionSelectorInterface, UidSelectorInterface
{
}