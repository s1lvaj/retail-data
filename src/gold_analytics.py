import logging
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldAnalytics:

    def __init__(self, spark_session: SparkSession, folder_loc="./data"):

        self.spark = spark_session
        self.base_path = Path(folder_loc)

        self.silver = self.base_path / "silver"
        self.gold = self.base_path / "gold"

    def build_sales_mart(self):

        orders = self.spark.read.parquet(str(self.silver / "orders"))
        items = self.spark.read.parquet(str(self.silver / "order_items"))

        sales = (
            orders.join(items, "order_id")
            .groupBy(F.to_date("order_purchase_timestamp").alias("order_date"))
            .agg(
                F.sum("price").alias("total_revenue"),
                F.countDistinct("order_id").alias("total_orders")
            )
        )

        sales.write.mode("overwrite").parquet(
            str(self.gold / "daily_sales")
        )

        logger.info("Gold sales mart created")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Layer").getOrCreate()

    gold = GoldAnalytics(spark)
    gold.build_sales_mart()