from pandas import date_range
from datetime import datetime, timedelta
import os.path as P
from itertools import product
from dateutil.relativedelta import relativedelta

from conf.general import PATH_ROOTS


def _expand_generation(aos_client, path, generation_filter=None, has_success=True):
    for gen_dir in reversed(aos_client.ls(path)):
        just_generation = gen_dir.rsplit("/", 1)[1]
        if generation_filter and just_generation > generation_filter:
            continue

        path_to_check = P.join(gen_dir, "_SUCCESS") if has_success else gen_dir
        path_to_check = aos_client.sparkify(path_to_check)
        if aos_client.exists(path_to_check):
            return just_generation

    print(f"no generation found under in {aos_client.provider}")


def resolve_algref_paths(aos_client, env, algorithm, date_ref):
    if env == "prd":
        root = PATH_ROOTS["prd"]["algref"]
    else:
        root = PATH_ROOTS["dev"]["algref"]

    client_algo_dir = P.join(root, algorithm)
    gen_filter = date_ref.strftime("%Y%m%d-%H%M%S") if date_ref else None
    generation = _expand_generation(aos_client, client_algo_dir, gen_filter)
    spark_path = aos_client.sparkify(P.join(client_algo_dir, generation))
    print(f"input: {spark_path}")

    return spark_path


def resolve_event_paths(aos_client, env, event, date_ref, n_months):
    if env == "prd":
        root = PATH_ROOTS["prd"]["events"]
    else:
        root = PATH_ROOTS["dev"]["events"]

    dates = date_range(end=date_ref, periods=n_months + 1, freq="MS")[:-1].date
    paths = [P.join(root, event, date.strftime("%Y-%m-%d")) for date in dates]
    spark_paths = [aos_client.sparkify(p) for p in paths if aos_client.exists(p)]

    if len(spark_paths) > 0:
        print(f"inputs: {len(spark_paths)} paths like {spark_paths[0]}")
        return spark_paths

    return "No objects found for buyorder"


def resolve_catalog_paths(aos_client, env, catalog, date_ref):
    if env == "prd":
        root = PATH_ROOTS["prd"]["catalog"]
    else:
        root = PATH_ROOTS["dev"]["catalog"]

    path = P.join(root, catalog)
    gen_filter = date_ref.strftime("%Y%m%d-%H%M%S") if date_ref else None
    generation = _expand_generation(aos_client, path, gen_filter)

    path = P.join(path, generation)
    spark_path = aos_client.sparkify(path)
    print(f"input: {spark_path}")
    return spark_path


def make_algref_path(aos_client, env, algorithm, generation):
    if env == "prd":
        root = PATH_ROOTS["prd"]["algref"]
    else:
        root = PATH_ROOTS["dev"]["algref"]

    apikey_algo_dir = P.join(root, algorithm, generation)
    spark_path = aos_client.sparkify(apikey_algo_dir)
    print(f"output: {spark_path}")

    return spark_path
