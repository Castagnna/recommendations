from pyspark.sql.types import *


_buyorder_item = StructType(
    [
        StructField(
            "product",
            StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("price", DoubleType(), True),
                ]
            ),
        ),
        StructField("quantity", DoubleType(), True),
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
        StructField("payment_type", StringType(), True),
    ]
)