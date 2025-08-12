-- src/create_tables.sql

-- Drop tables if they exist to ensure a clean slate for recreation
DROP TABLE IF EXISTS fact_order_items;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_sellers;
DROP TABLE IF EXISTS dim_geolocation;

-- Dimension table for customers
CREATE TABLE dim_customers (
    customer_unique_id VARCHAR(32) PRIMARY KEY,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(256),
    customer_state VARCHAR(2)
) DISTSTYLE ALL; -- Small table, replicate on all nodes for faster joins

-- Dimension table for products
CREATE TABLE dim_products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(256),
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
) DISTSTYLE ALL; -- Small table, replicate on all nodes

-- Dimension table for sellers
CREATE TABLE dim_sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(256),
    seller_state VARCHAR(2)
) DISTSTYLE ALL; -- Small table, replicate on all nodes

-- Dimension table for geolocation
CREATE TABLE dim_geolocation (
    geolocation_zip_code_prefix INT PRIMARY KEY,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT
) DISTSTYLE ALL; -- Small reference table, replicate on all nodes

-- Fact table for order items
CREATE TABLE fact_order_items (
    order_id VARCHAR(32),
    order_item_id SMALLINT,
    product_id VARCHAR(32) REFERENCES dim_products(product_id),
    seller_id VARCHAR(32) REFERENCES dim_sellers(seller_id),
    customer_unique_id VARCHAR(32) REFERENCES dim_customers(customer_unique_id),
    order_purchase_timestamp TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    review_score SMALLINT,
    PRIMARY KEY(order_id, order_item_id)
)
DISTKEY (order_id) -- Distribute by order_id to co-locate items of the same order
SORTKEY(order_purchase_timestamp); -- Sort by timestamp for efficient time-series analysis