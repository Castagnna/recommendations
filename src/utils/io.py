from schemas import get_schema
from utils.paths import (
    resolve_event_paths, resolve_catalog_paths, make_algref_path
)


def read_event_dumps(
    spark,
    caos_client,
    event,
    env="prd",
    select_fields=None,
    drop_fields=None,
    custom_schema=None,
    dry_run=False,
    **range_kwargs
):
    paths = resolve_event_paths(
        caos_client, env, event, **range_kwargs
    )
    schema = custom_schema or get_schema("event", event, select_fields, drop_fields)

    if dry_run:
        return

    return spark.read.json(paths, schema=schema)


def read_catalog(
    spark,
    caos_client,
    date_ref,
    env="prd",
    select_fields=None,
    drop_fields=None,
    custom_schema=None,
    dry_run=False,
):
    paths = resolve_catalog_paths(caos_client, env, date_ref)

    schema = custom_schema or get_schema(
        "catalog", "products", select_fields, drop_fields
    )

    if dry_run:
        return
    return spark.read.json(paths, schema=schema)


def write_dump(
    caos_client,
    dataframe,
    algorithm,
    generation,
    dry_run=False,
):
    output_path = make_algref_path(caos_client, algorithm, generation)
    if dry_run:
        return

    (
        dataframe
        .write.format("parquet")
        .option("compression", "gzip")
        .mode("overwrite")
        .save(output_path)
    )
