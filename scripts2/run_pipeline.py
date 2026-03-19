from pyspark.sql import SparkSession
from src.bronze_ingestion import BronzeIngestion
from src.silver_cleaning import SilverCleaning
from src.gold_analytics import GoldAnalytics

spark = SparkSession.builder.appName("Olist Pipeline").getOrCreate()

bronze = BronzeIngestion(spark)
silver = SilverCleaning(spark)
gold = GoldAnalytics(spark)

datasets = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_order_payments_dataset"
]

for ds in datasets:
    bronze.ingest_csv(ds)

silver.clean_orders()
silver.clean_order_items()

gold.build_sales_mart()