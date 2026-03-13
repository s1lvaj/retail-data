# Mock Retail-Company Data

Data science and engineering on a mock retail company.

Just a reminder that you may have trouble running pyspark while using a VPN.´

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