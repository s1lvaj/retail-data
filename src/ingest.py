from pathlib import Path
import pandas as pd
import logging

# -----------------------------
# Configuration
# -----------------------------

RAW_DATA_PATH = Path("data/raw")
BRONZE_DATA_PATH = Path("data/bronze")

DATASETS = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_customers_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_geolocation_dataset",
]

# -----------------------------
# Logging setup
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# -----------------------------
# Core Functions
# -----------------------------

def read_csv(dataset_name: str) -> pd.DataFrame:
    """Read dataset from raw layer."""
    file_path = RAW_DATA_PATH / f"{dataset_name}.csv"

    logger.info(f"Reading {file_path}")
    df = pd.read_csv(file_path)

    logger.info(f"{dataset_name}: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


def basic_validation(df: pd.DataFrame, dataset_name: str):
    """Simple validation checks."""
    if df.empty:
        raise ValueError(f"{dataset_name} is empty!")

    duplicate_rows = df.duplicated().sum()
    if duplicate_rows > 0:
        logger.warning(f"{dataset_name} has {duplicate_rows} duplicate rows")


def write_parquet(df: pd.DataFrame, dataset_name: str):
    """Write dataframe to bronze layer."""
    BRONZE_DATA_PATH.mkdir(parents=True, exist_ok=True)

    output_path = BRONZE_DATA_PATH / f"{dataset_name}.parquet"

    logger.info(f"Writing parquet → {output_path}")
    df.to_parquet(output_path, index=False)


def process_dataset(dataset_name: str):
    """Full ingestion pipeline for one dataset."""
    df = read_csv(dataset_name)
    basic_validation(df, dataset_name)
    write_parquet(df, dataset_name)


# -----------------------------
# Pipeline Entry Point
# -----------------------------

def main():
    logger.info("Starting ingestion pipeline")

    for dataset in DATASETS:
        process_dataset(dataset)

    logger.info("Ingestion pipeline completed successfully")


if __name__ == "__main__":
    main()