from pyspark.sql.types import *


# algorithms outputs

## similaritems
_similaritems_results = ArrayType(
    StructType(
        [
            StructField("rec_id", StringType(), True),
            StructField("nsl", DoubleType(), True),
            StructField("asl", DoubleType(), True),
            StructField("amf", DoubleType(), True),
            StructField("nmf", DoubleType(), True),
            StructField("rec_fre", DoubleType(), True),
            StructField("ref_fre", DoubleType(), True),
        ]
    ),
    False,
)

_similaritems_tuning = StructType(
    [
        StructField("adjust", DoubleType(), True),
        StructField("alpha", DoubleType(), True),
        StructField("amf", DoubleType(), True),
        StructField("nmf", DoubleType(), True),
    ]
)

similaritems = StructType(
    [
        StructField("client", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("results", _similaritems_results, False),
        StructField("tuning", _similaritems_tuning, True),
    ]
)
