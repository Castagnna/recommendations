# Hybrid recommendation systems: buy order collaborative filtering
# Considers purchases from the same user, in the same order.

from functools import partial
from datetime import datetime
from dateutil import parser as dateparser
from utils.common import pipe
from utils.spark import start_spark
from utils.aos import AOS_client
from utils.io import (
    read_event_dumps,
    read_catalog,
    write_dump,
)
from algorithms.similaritems.functions import (
    filter_items,
    select_buyorder_columns,
    create_artificial_id,
    filter_available_or_unavailable_items,
    count_distinct_users_per_item,
    count_items_from_same_user_id_for_distinct_orders,
    wipe_out_users_and_items_that_appear_only_once,
    calculate_coincidence,
    mirror_and_union,
    calculate_asl_nsl_and_ranking_recs,
    agg_and_sort_results,
    format_output,
)
from algorithms.similaritems.config import TUNINGS


def run(
    products,
    buyorders,
    user_events_limit,
    min_amf,
    min_nmf,
    alpha,
    adjust,
    max_number_of_similars,
):
    products = filter_items(products)

    buyorders = pipe(select_buyorder_columns, create_artificial_id)

    valid_buyorders = filter_available_or_unavailable_items(
        buyorders,
        products,
    )

    item_freq = count_distinct_users_per_item(valid_buyorders)
    user_freq = count_items_from_same_user_id_for_distinct_orders(valid_buyorders)

    return pipe(
        valid_buyorders,
        partial(
            wipe_out_users_and_items_that_appear_only_once,
            user_freq=user_freq,
            item_freq=item_freq,
            max_events_per_user=user_events_limit,
        ),
        partial(
            calculate_coincidence,
            user_freq=user_freq,
            item_freq=item_freq,
            min_amf=min_amf,
            min_nmf=min_nmf,
        ),
        mirror_and_union,
        partial(
            calculate_asl_nsl_and_ranking_recs,
            alpha=alpha,
            adjust=adjust,
            max_number_of_similars=max_number_of_similars,
        ),
        agg_and_sort_results,
        partial(
            format_output,
            min_nmf=min_nmf,
            min_amf=min_amf,
            alpha=alpha,
            adjust=adjust,
        ),
    )


def setup(env="prd", provider="os", date_ref="today", dry_run=False, **tunings):
    job_start_dttm = datetime.now()

    SIMILARITEMS_ALGO = "si-cf-buyorder"

    deploy_mode = "cluster" if env == "prd" else "standalone"
    spark = start_spark(deploy_mode)
    aos = AOS_client(provider)

    if date_ref == "today":
        date_ref = job_start_dttm.date()
    else:
        date_ref = dateparser.parse(date_ref)

    if env == "prd":
        input_block = "all"
    else:
        input_block = ""

    products = read_catalog(
        spark,
        aos,
        "products",
        date_ref,
        input_block,
        env,
        select_fields=["client", "product_id", "status"],
        dry_run=dry_run,
    )

    buyorders = read_event_dumps(
        spark,
        aos,
        "buyorder",
        env=env,
        select_fields=["client", "order_id", "items", "user_id"],
        dry_run=dry_run,
        end=date_ref,
        start=date_ref.replace(day=1),
    )

    output = None
    if not dry_run:
        output = run(
            products,
            buyorders,
            TUNINGS.get("max_input_events_from_user"),
            TUNINGS.get("buyorder_absolute_mutual_frequency"),
            TUNINGS.get("buyorder_normalized_mutual_frequency"),
            TUNINGS.get("buyorder_alpha"),
            TUNINGS.get("buyorder_adjust"),
            TUNINGS.get("max_number_of_similars"),
        )

    generation = job_start_dttm.strftime("%Y%m%d-%H%M%S")
    output_block = "all" if env == "prd" else env
    write_dump(
        aos, output, SIMILARITEMS_ALGO, generation, output_block, dry_run=dry_run
    )
