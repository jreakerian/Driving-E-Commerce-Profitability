-- src/sql/create_tables.sql

-- Drop tables if they exist to ensure a clean slate for recreation
-- Use CASCADE to automatically drop dependent objects like materialized views.
DROP TABLE IF EXISTS fact_order_items CASCADE;
DROP TABLE IF EXISTS dim_orders CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_sellers CASCADE;
DROP TABLE IF EXISTS dim_geolocation CASCADE;

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

    -- NEW: Dimension table for orders
CREATE TABLE dim_orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32),
    order_status VARCHAR(32),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
)
DISTKEY (order_id)  -- Distribute by order_id as it will be joined to the fact table
SORTKEY(order_purchase_timestamp); -- Sort by purchase timestamp for time-based analysis

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
    payment_value DECIMAL(10, 2),
    payment_installments SMALLINT,
    payment_type VARCHAR(32),
    review_score SMALLINT
)
DISTKEY (order_id) -- Distribute by order_id to co-locate items of the same order
SORTKEY(order_purchase_timestamp); -- Sort by timestamp for efficient time-series analysis

-- Drop the materialized view if it already exists to ensure a clean start
DROP MATERIALIZED VIEW IF EXISTS customer_rfm_analysis_mv;

-- Create the materialized view, wrapping the complex query to resolve ambiguity
CREATE MATERIALIZED VIEW customer_rfm_analysis_mv
BACKUP YES
DISTSTYLE KEY
DISTKEY (customer_unique_id)
SORTKEY (monetary_score, frequency_score, recency_score)
AS
SELECT * FROM (
    WITH rfm_base AS (
        SELECT
            f.customer_unique_id,
            -- Calculate Recency: days since last purchase from the last date in the dataset
            (SELECT MAX(order_purchase_timestamp)::date FROM fact_order_items) - MAX(f.order_purchase_timestamp)::date AS recency,
            -- Calculate Frequency: total number of distinct orders
            COUNT(DISTINCT f.order_id) AS frequency,
            -- Calculate Monetary: total revenue from the customer
            SUM(f.price + f.freight_value) AS monetary
        FROM fact_order_items AS f
        GROUP BY 1
    ),
    -- Assign scores from 1 to 5 based on quintiles
    rfm_scores AS (
        SELECT
            customer_unique_id,
            recency,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency ASC) AS recency_score,
            NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
            NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_score
        FROM rfm_base
    )
    -- Final selection of customer RFM profiles, joining with customer details
    SELECT
        s.customer_unique_id,
        c.customer_city,
        c.customer_state,
        s.recency,
        s.frequency,
        s.monetary,
        s.recency_score,
        s.frequency_score,
        s.monetary_score,
        -- Concatenate scores for an easy-to-read segment ID
        CAST(s.recency_score AS VARCHAR) || CAST(s.frequency_score AS VARCHAR) || CAST(s.monetary_score AS VARCHAR) AS rfm_segment
    FROM rfm_scores AS s
    JOIN dim_customers AS c ON s.customer_unique_id = c.customer_unique_id
) AS rfm_final_results; -- This alias is also required for the derived table

--REFRESH MATERIALIZED VIEW customer_rfm_analysis_mv;

SELECT
    le.starttime,
    le.err_reason,
    le.raw_line,
    le.raw_field_value,
    le.colname,
    le.type,
    le.col_length
FROM stl_load_errors le
ORDER BY le.starttime DESC
LIMIT 1;