-- FACT TABLE
CREATE OR REPLACE TABLE fact_orders AS
SELECT
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_purchase_timestamp AS order_date,
    oi.price,
    oi.freight_value
FROM read_parquet('data/silver/order_items.parquet') oi
JOIN read_parquet('data/silver/orders.parquet') o
ON oi.order_id = o.order_id;


-- DIM CUSTOMERS
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    customer_id,
    customer_city,
    customer_state
FROM read_parquet('data/silver/customers.parquet');


-- DIM DATE
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    order_date,
    year(order_date) AS year,
    month(order_date) AS month,
    day(order_date) AS day
FROM fact_orders;