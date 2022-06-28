import importlib
from pyspark.sql.types import StructType, ArrayType


def drop_field(schema, field):
    if field.count(".") == 0:
        return StructType([x for x in schema if x.name != field])
    else:
        superfield, _, subfields = field.partition(".")
        if schema[superfield].dataType.typeName() == "array":
            subschema = schema[superfield].dataType.elementType
            new_subschema = ArrayType(drop_field(subschema, subfields))
        else:
            subschema = schema[superfield].dataType
            new_subschema = drop_field(subschema, subfields)

        schema[superfield].dataType = new_subschema
        return schema


def _filter_fields(schema, select_fields: list = [], drop_fields: list = []):
    if select_fields:
        schema = StructType([field for field in schema if field.name in select_fields])

    if drop_fields:
        for field in drop_fields:
            schema = drop_field(schema, field)

    return schema


def get_schema(
    source: str,
    name: str,
    select_fields: list = [],
    drop_fields: list = [],
):
    try:
        module = importlib.import_module(f"schemas.{source}")
    except ModuleNotFoundError as e:
        # TODO: list available sources (via inspection)
        raise e

    try:
        schema = getattr(module, name)
    except AttributeError as e:
        # TODO: list available schemas in that module (via inspection)
        raise e

    return _filter_fields(schema, select_fields, drop_fields)
