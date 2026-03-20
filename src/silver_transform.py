from pathlib import Path
import pandas as pd
import logging

BRONZE = Path("data/bronze")
SILVER = Path("data/silver")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_orders():
    df = pd.read_parquet(BRONZE / "olist_orders_dataset.parquet")

    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )

    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id"])

    return df


def clean_order_items():
    df = pd.read_parquet(BRONZE / "olist_order_items_dataset.parquet")

    df["price"] = df["price"].fillna(0)
    df["freight_value"] = df["freight_value"].fillna(0)

    return df


def clean_customers():
    df = pd.read_parquet(BRONZE / "olist_customers_dataset.parquet")
    return df.drop_duplicates(subset=["customer_id"])


def write(df, name):
    SILVER.mkdir(parents=True, exist_ok=True)
    path = SILVER / f"{name}.parquet"
    logger.info(f"Writing {path}")
    df.to_parquet(path, index=False)


def main():
    write(clean_orders(), "orders")
    write(clean_order_items(), "order_items")
    write(clean_customers(), "customers")


if __name__ == "__main__":
    main()