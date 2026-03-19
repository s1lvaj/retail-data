from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailAggregation")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/order_items.parquet")

result = (
    df.groupBy("product_id")
      .sum("price")
      .orderBy("sum(price)", ascending=False)
)

result.write.mode("overwrite").parquet("data/gold/top_products")

spark.stop()