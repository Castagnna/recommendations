from pyspark.sql.types import *


products = StructType(
    [
        StructField("client", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)
