# Mock Retail-Company Data

In this repository we build a **batch data pipeline** that ingests messy retail data, processes it, models it, and produces analytics tables. It serves as a reference tool, simulating a real company architecture.

In this project, we try to cover concepts like data lakes, batch pipelines, streaming basics, clusters, distributed compute, SQL transformations, etc.

## Project Structure

```bash
retail-data/
├── data/                      # Datasets
│   ├── raw/                   # Original untouched data (bronze layer)
│   ├── cleaned/               # Fixed data (silver layer)
│   └── curated/               # Analytics-ready data (gold layer)
├── src/
├── sql/
├── pipeline.py
├── LICENSE                    # MIT License
├── README.md                  # General README information
└── requirements.txt           # External libraries to be installed with Python
```

## Dataset

This project uses the Brazilian E‑Commerce Public Dataset by Olist. You can download all the csv files we need directly from github by using:

```bash
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv -OutFile olist_orders_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_items_dataset.csv -OutFile olist_order_items_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_customers_dataset.csv -OutFile olist_customers_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_products_dataset.csv -OutFile olist_products_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_sellers_dataset.csv -OutFile olist_sellers_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_payments_dataset.csv -OutFile olist_order_payments_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_reviews_dataset.csv -OutFile olist_order_reviews_dataset.csv
wget https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_geolocation_dataset.csv -OutFile olist_geolocation_dataset.csv
```

The csv files are saved in `/data/raw`.

## Step 2

ingest.py

## Step 3

silver_transform.py

## Step 4

sql/gold_models.sql

## Step 5

build_gold.py

## Step 6

Distributed Compute Example:

spark_job.py

Just a reminder that you may have trouble running pyspark while using a VPN.

(our dataset is small, so pandas is enough, but this is to understand distributed compute, lazy execution, spark transformations)

## Step 7

Pipeline Orchestration

pipeline.py

## How to Use

1. **Clone the Repository**:

    ```bash
    git clone https://github.com/s1lvaj/retail-data.git
    ```

2. **Install the Requirements**:

    ```bash
    pip install -r requirements.txt
    ```

3. **Run the Pipeline Script**:

    ```bash
    python pipeline.py
    ```