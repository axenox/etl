<?php
namespace axenox\ETL\Facades\Helper;

use exface\Core\DataTypes\StringDataType;
use exface\Core\Interfaces\Model\MetaObjectInterface;
use exface\Core\DataTypes\IntegerDataType;
use exface\Core\DataTypes\NumberDataType;
use exface\Core\DataTypes\BooleanDataType;
use exface\Core\DataTypes\ArrayDataType;
use exface\Core\Interfaces\DataTypes\EnumDataTypeInterface;
use exface\Core\DataTypes\DateTimeDataType;
use exface\Core\DataTypes\DateDataType;
use exface\Core\DataTypes\BinaryDataType;
use exface\Core\Exceptions\InvalidArgumentException;

/**
 * A builder that creates a schema from a MetaObjectInterface.
 * 
 * @author miriam.seitz
 *
 */
class MetaModelSchemaBuilder
{
	/**
	 * Create a json schema for the given meta object. Uses all DataType classes supported by JsonSchema.
	 * 
	 * @param MetaObjectInterface $metaobject
	 * @param array $attributeAliasesToAdd
	 */
	public static function transformIntoJsonSchema(MetaObjectInterface $metaobject, array $attributeAliasesToAdd): array
	{
		$objectName = $metaobject->getAliasWithNamespace();
		$jsonSchema = [$objectName => ['type' => 'object', 'properties' => []]];
		
		foreach ($metaobject->getAttributes() as $attribute) {
			$dataType = $attribute->getDataType();
			switch (true) {
				case $attribute->isRelation():
					$relatedObjectAlias = $attribute->getRelation()
					->getRightObject()
					->getAliasWithNamespace();
					if (in_array($relatedObjectAlias, $attributeAliasesToAdd)) {
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
			
			if ($attribute->getHint() !== $attribute->getName()) {
				$schema['description'] = $attribute->getHint();
			}
			
			$jsonSchema[$objectName]['properties'][$attribute->getAlias()] = $schema;
		}
		
		return $jsonSchema;
	}
}