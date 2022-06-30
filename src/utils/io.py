from schemas import get_schema
from utils.paths import resolve_event_paths, resolve_catalog_paths, make_algref_path


def read_event_dumps(
    spark,
    aos_client,
    event,
    env="prd",
    select_fields=None,
    drop_fields=None,
    custom_schema=None,
    dry_run=False,
    **range_kwargs
):
    paths = resolve_event_paths(aos_client, env, event, **range_kwargs)
    schema = custom_schema or get_schema("event", event, select_fields, drop_fields)

    if dry_run:
        return

    return spark.read.json(paths, schema=schema)


def read_catalog(
    spark,
    aos_client,
    catalog,
    date_ref,
    env="prd",
    select_fields=None,
    drop_fields=None,
    custom_schema=None,
    dry_run=False,
):
    paths = resolve_catalog_paths(aos_client, env, catalog, date_ref)

    schema = custom_schema or get_schema(
        "catalog", "products", select_fields, drop_fields
    )

    if dry_run:
        return
    return spark.read.json(paths, schema=schema)


def write_dump(
    aos_client,
    env,
    dataframe,
    algorithm,
    generation,
    dry_run=False,
):
    output_path = make_algref_path(aos_client, env, algorithm, generation)
    if dry_run:
        return

    (
        dataframe.write.format("json")
        .option("compression", "gzip")
        .mode("overwrite")
        .save(output_path)
    )
