import logging
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverCleaning:

    def __init__(self, spark_session: SparkSession, folder_loc="./data"):

        self.spark = spark_session
        self.base_path = Path(folder_loc)

        self.bronze = self.base_path / "bronze"
        self.silver = self.base_path / "silver"

    def clean_orders(self):

        df = self.spark.read.parquet(str(self.bronze / "olist_orders_dataset"))

        df_clean = (
            df
            .withColumn(
                "order_purchase_timestamp",
                F.to_timestamp("order_purchase_timestamp")
            )
            .dropDuplicates(["order_id"])
            .dropna(subset=["order_id", "customer_id"])
        )

        df_clean.write.mode("overwrite").parquet(
            str(self.silver / "orders")
        )

        logger.info("Orders cleaned")

    def clean_order_items(self):

        df = self.spark.read.parquet(
            str(self.bronze / "olist_order_items_dataset")
        )

        df_clean = (
            df
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("freight_value", F.col("freight_value").cast("double"))
            .dropDuplicates()
        )

        df_clean.write.mode("overwrite").parquet(
            str(self.silver / "order_items")
        )

        logger.info("Order items cleaned")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Cleaning").getOrCreate()

    cleaner = SilverCleaning(spark)

    cleaner.clean_orders()
    cleaner.clean_order_items()