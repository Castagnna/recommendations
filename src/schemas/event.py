from pyspark.sql.types import *


_buyorder_item = StructType(
    [
        StructField(
            "product",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("specs", MapType(StringType(), StringType()), True),
                ]
            ),
        ),
    ]
)

buyorder = StructType(
    [
        StructField("client", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("items", ArrayType(_buyorder_item, True), True),
        StructField("date", StringType(), True),
        StructField("total", DoubleType(), True),
    ]
)
