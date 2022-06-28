from pyspark.sql import functions as F, Window as W
from pyspark.sql import DataFrame


def filter_items(items) -> DataFrame:
    return items.where(F.col("status").isin("AVAILABLE", "UNAVAILABLE")).select(
        "client",
        F.col("product_id").alias("ITEM"),
    )


def select_purchase_columns(events) -> DataFrame:
    return events.withColumn("item", F.explode("items").alias("item"),).select(
        "client",
        # LINK in algorithm recommendation context refers to the atribute which link items
        F.col("user_id").alias("LINK"),
        F.col("item")["product"]["product_id"].alias("ITEM"),
    )


def select_buyorder_columns(events) -> DataFrame:
    return events.withColumn("item", F.explode("items").alias("item"),).select(
        F.col("client").alias("client"),
        F.col("user_id").alias("user_id"),
        # in buyorder case order_id is the LINK
        F.col("order_id").alias("LINK"),
        F.col("item")["product"]["product_id"].alias("ITEM"),
    )


def create_artificial_id(all_buyorder_events) -> DataFrame:
    """
    To avoid identical order identifiers coming from different users.
    Just apply for buyorder modality
    """
    return all_buyorder_events.withColumn("LINK", F.concat("user_id", "LINK"))


def filter_available_or_unavailable_items(events, valid_items):
    return events.join(valid_items, how="inner", on=["client", "ITEM"]).distinct()


def count_distinct_items_per_user(valid_events) -> DataFrame:
    """
    Count distinct items per user. Used by purchase algo.
    """
    return valid_events.groupby(["client", "LINK"]).agg(
        F.countDistinct("ITEM").alias("ufrq")
    )


def count_items_from_same_user_id_for_distinct_orders(valid_events) -> DataFrame:
    """
    Used by opsi buyorder algo.
    """

    items_per_user_id = valid_events.groupby(["client", "user_id"]).agg(
        F.count("ITEM").alias("ufrq")
    )

    return (
        valid_events.select("client", "user_id", "LINK")
        .distinct()
        .join(items_per_user_id, on=["client", "user_id"])
    )


def count_distinct_users_per_item(valid_events) -> DataFrame:
    return valid_events.groupBy("client", "ITEM").agg(
        F.countDistinct("LINK").alias("freq")
    )


def wipe_out_users_and_items_that_appear_only_once(
    valid_events, user_freq, item_freq, max_events_per_user
) -> DataFrame:

    item_freq = item_freq.where("freq > 1")

    user_freq = user_freq.where(F.col("ufrq").between(2, max_events_per_user))

    return (
        valid_events.join(user_freq, on=["client", "LINK"], how="inner")
        .join(item_freq, on=["client", "ITEM"], how="inner")
        .select("client", "LINK", "ITEM")
        .distinct()
    )


def calculate_coincidence(
    prepared_events, user_freq, item_freq, min_amf, min_nmf
) -> DataFrame:

    prepared_events_rec = prepared_events.withColumnRenamed("ITEM", "item_rec")

    prepared_events_ref = prepared_events.withColumnRenamed("ITEM", "item_ref")

    item_freq_ref = item_freq.withColumnRenamed("ITEM", "item_ref").withColumnRenamed(
        "freq", "freq_ref"
    )

    item_freq_rec = item_freq.withColumnRenamed("ITEM", "item_rec").withColumnRenamed(
        "freq", "freq_rec"
    )

    return (
        prepared_events_rec.join(
            prepared_events_ref, on=["client", "LINK"], how="inner"
        )
        .where("item_ref < item_rec")
        .join(user_freq, on=["client", "LINK"], how="inner")
        # normalize column "ufrq", such that 0 < "norm" <= 1
        .withColumn("norm", 2 / F.col("ufrq"))
        .groupBy("client", "item_ref", "item_rec")
        .agg(
            F.count("item_ref").alias("amf"),
            F.sum("norm").alias("nmf"),
        )
        .where((F.col("amf") >= min_amf) & (F.col("nmf") >= min_nmf))
        .join(item_freq_ref, on=["client", "item_ref"], how="inner")
        .join(item_freq_rec, on=["client", "item_rec"], how="inner")
    )


def mirror_and_union(coincidence) -> DataFrame:

    coincidence_mirror = coincidence.select(
        "client",
        F.col("item_ref").alias("item_rec"),
        F.col("item_rec").alias("item_ref"),
        "amf",
        "nmf",
        F.col("freq_rec").alias("freq_ref"),
        F.col("freq_ref").alias("freq_rec"),
    )

    return coincidence.union(coincidence_mirror)


def _similarityof(nmf, f_rec, f_ref, alpha=0.1, const_adjust=2):
    """
    Absolute similarity level (ASL) is the metric that indicate the relevance
    of the recommendation
    """

    nmf = F.col(nmf)
    f_rec = F.col(f_rec)
    f_ref = F.col(f_ref)
    sim_base = nmf / (f_ref * F.pow(f_rec, alpha))
    adjustment = (nmf * f_ref) / F.pow(f_rec, 2)
    exp_value = F.exp(-const_adjust * ((adjustment * 100) - 1))
    similarityof = sim_base * (2 / (1 + exp_value))

    return similarityof


def calculate_asl_nsl_and_ranking_recs(
    coincidence_mirrored,
    alpha,
    adjust,
    max_number_of_similars,
) -> DataFrame:

    rank_window = W.partitionBy("client", "item_ref").orderBy(
        F.col("client"),
        F.col("item_ref"),
        F.col("asl").desc(),
        F.col("item_rec").desc(),
        F.col("freq_rec").desc(),
    )

    nsl_window = W.partitionBy("client", "item_ref")

    return (
        coincidence_mirrored.withColumn(
            "asl", _similarityof("nmf", "freq_rec", "freq_ref", alpha, adjust)
        )
        .withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= max_number_of_similars)
        .withColumn("nsl", F.col("asl") / F.max("asl").over(nsl_window))
    )


def agg_and_sort_results(exploded_recommendations) -> DataFrame:

    result = F.struct(
        F.col("item_rec").alias("rec"),
        F.col("amf"),
        F.col("nmf"),
        F.col("freq_rec"),
        F.col("freq_ref"),
        F.col("asl"),
        F.col("nsl"),
    ).alias("result")

    result_by_nsl = F.collect_list(F.struct("nsl", result))

    return (
        exploded_recommendations.groupBy("client", "item_ref")
        .agg(
            # sort_array will sort the array by first struct column: nsl
            F.sort_array(result_by_nsl, asc=False).alias("nsl_result"),
        )
        .withColumn("results", F.col("nsl_result.result"))
    )


def format_output(
    recommendations_sorted,
    min_nmf,
    min_amf,
    alpha,
    adjust,
) -> DataFrame:

    tuning = F.struct(
        F.lit(min_nmf).alias("nmf"),
        F.lit(alpha).alias("alpha"),
        F.lit(adjust).alias("adjust"),
        F.lit(min_amf).alias("amf"),
    )

    return recommendations_sorted.select(
        F.col("client").alias("client"),
        tuning.alias("tuning"),
        F.col("item_ref").alias("product"),
        F.col("results"),
    )
