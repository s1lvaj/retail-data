import logging
from pathlib import Path
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeIngestion:

    def __init__(
        self,
        spark_session: SparkSession,
        folder_loc: str = "./data"
    ):
        self.spark = spark_session
        self.base_path = Path(folder_loc)

        self.raw_path = self.base_path / "raw"
        self.bronze_path = self.base_path / "bronze"

    def ingest_csv(self, dataset_name: str):

        input_path = self.raw_path / f"{dataset_name}.csv"
        output_path = self.bronze_path / dataset_name

        logger.info(f"Ingesting {dataset_name}")

        df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(input_path))
        )

        df.write.mode("overwrite").parquet(str(output_path))

        logger.info(f"{dataset_name} written to bronze layer")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

    bronze = BronzeIngestion(spark)

    datasets = [
        "olist_orders_dataset",
        "olist_order_items_dataset",
        "olist_customers_dataset",
        "olist_products_dataset",
        "olist_order_payments_dataset"
    ]

    for ds in datasets:
        bronze.ingest_csv(ds)