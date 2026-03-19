import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetailData:

    def __init__(
        self,
        spark_session: SparkSession,
        folder_loc: str = "./"
    ):
        """
        Retail data cleaner for Olist dataset (Silver layer).
        """

        self.base_path = Path(folder_loc)

        # Medallion layers
        self.bronze_path = self.base_path / "data" / "bronze"
        self.output_path = self.base_path / "data" / "silver"

        self.spark = spark_session

        # Load datasets
        self.orders = self._load_dataset("olist_orders_dataset")
        self.order_items = self._load_dataset("olist_order_items_dataset")
        self.customers = self._load_dataset("olist_customers_dataset")

        # Clean
        self._clean_data()

    # -----------------------------
    # Load
    # -----------------------------
    def _load_dataset(self, name: str) -> DataFrame:
        path = self.bronze_path / f"{name}.parquet"

        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")

        logger.info(f"Loading {path}")
        return self.spark.read.parquet(str(path))

    # -----------------------------
    # Cleaning
    # -----------------------------
    def _clean_data(self):

        logger.info("Cleaning orders dataset")

        self.orders = (
            self.orders
            .withColumn(
                "order_purchase_timestamp",
                F.to_timestamp(col("order_purchase_timestamp"))
            )
            .dropDuplicates(["order_id"])
            .dropna(subset=["order_id", "customer_id"])
        )

        logger.info("Cleaning order_items dataset")

        self.order_items = (
            self.order_items
            .withColumn("price", col("price").cast("double"))
            .withColumn("freight_value", col("freight_value").cast("double"))
            .fillna({"price": 0.0, "freight_value": 0.0})
        )

        logger.info("Cleaning customers dataset")

        self.customers = (
            self.customers
            .dropDuplicates(["customer_id"])
        )

        logger.info("Cleaning completed")

    # -----------------------------
    # Save
    # -----------------------------
    def save_data(self, format: str = "parquet"):

        self.output_path.mkdir(parents=True, exist_ok=True)

        logger.info("Writing Silver datasets")

        self.orders.write.mode("overwrite").format(format).save(
            str(self.output_path / "orders")
        )

        self.order_items.write.mode("overwrite").format(format).save(
            str(self.output_path / "order_items")
        )

        self.customers.write.mode("overwrite").format(format).save(
            str(self.output_path / "customers")
        )

        logger.info("Silver layer successfully written")


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Olist Silver Cleaning")
        .getOrCreate()
        # .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # this could be included to signal performance awareness
    )

    retail_data = RetailData(spark_session=spark, folder_loc="./")
    retail_data.save_data("parquet")

    spark.stop()