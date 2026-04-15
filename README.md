# Mock Retail-Company Data

This repository implements a batch data pipeline that ingests raw retail data, processes it through multiple layers, and produces analytics-ready tables.

It is designed as a simplified simulation of a real-world data platform, showcasing core data engineering concepts such as data modeling, transformation layers, and pipeline orchestration.

## Project Structure

```bash
retail-data/
├── data/                      # Datasets
│   └── raw/                   # Original untouched data
├── docker/                    # Docker configuration
├── sql/                       # SQL transformations (gold layer)
├── src/                       # Python scripts
├── pipeline.py                # End-to-end pipeline orchestration
├── LICENSE                    # MIT License
├── README.md                  # Project documentation
└── requirements.txt           # Python dependencies
```

During code execution, the following folders will be created:

- `data/bronze` containing raw data converted to Parquet.
- `data/silver` containing cleaned and transformed data.
- `data/gold` containing curated, analytics-ready tables.

This project follows a multi-layer data architecture, with the bronze, silver and gold layers above mentioned, and it loosely follows a lakehouse-style pipeline, using layered transformations on top of raw data.

Data Lakes vs Warehouses vs Lakehouses:

- Data Lake: Raw, unstructured data storage (cheap, flexible).
- Data Warehouse: Structured, optimized for analytics (fast queries, strict schema).
- Lakehouse: Hybrid—combines lake flexibility with warehouse performance and structure.

## Dataset

This project uses the Brazilian E‑Commerce Public Dataset by Olist.

Download the required files into `data/raw`:

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

## Pipeline Steps

1. Ingestion

    `src/bronze_ingestion.py`
    - Loads raw CSV files
    - Converts data into Parquet (Bronze layer)

2. Transformation (Silver)

    `src/silver_cleaning.py`
    - Cleans and standardizes datasets

3. Modeling (Gold)

    `sql/gold_models.sql`
    - Defines analytics tables using SQL

4. Gold Build

    `src/gold_analytics.py`
    - Executes SQL transformations

5. Orchestration

    `pipeline.py`
    - Runs the full pipeline end-to-end

## Tech Stack

- **Python**: Core pipeline logic
- **SQL**: Data modeling (gold layer)
- **PySpark**: Distributed processing example
- **Parquet**: Columnar storage format

Pandas would be sufficient for this dataset, but PySpark and SQL are included to demonstrate scalable data engineering patterns.

Note: PySpark may not run correctly when using a VPN.

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