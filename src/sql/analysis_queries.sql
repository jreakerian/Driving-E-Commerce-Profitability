-- src/sql/analysis_queries.sql

-- Query 1: Key Performance Indicators (KPIs)
-- This query calculates high-level business metrics like total revenue,
-- total orders, and average order value (AOV).
SELECT
    'KPIs' AS analysis_type,
    SUM(price + freight_value) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(price + freight_value) / COUNT(DISTINCT order_id) AS average_order_value
FROM fact_order_items;

-- Query 2: Sales by Customer State
-- This query aggregates total revenue by customer state to identify top markets.

SELECT
    'Sales by State' AS analysis_type,
    c.customer_state,
    SUM(f.price + f.freight_value) AS total_revenue
FROM fact_order_items f
JOIN dim_customers c ON f.customer_unique_id = c.customer_unique_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 3: RFM (Recency, Frequency, Monetary) Analysis
-- This query segments customers based on their purchasing behavior to identify high-value customers.
-- Note: This is a multi-step query using Common Table Expressions (CTEs).

WITH rfm_base AS (
    SELECT
        customer_unique_id,
        -- Calculate Recency: days since last purchase from the last date in the dataset
        (SELECT MAX(order_purchase_timestamp)::date FROM fact_order_items) - MAX(order_purchase_timestamp)::date AS recency,
        -- Calculate Frequency: total number of distinct orders
        COUNT(DISTINCT order_id) AS frequency,
        -- Calculate Monetary: total revenue from the customer
        SUM(price + freight_value) AS monetary
    FROM fact_order_items
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
-- Final selection of top 10 high-value customers
SELECT
    'Top RFM Customers' AS analysis_type,
    customer_unique_id,
    recency,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score
FROM rfm_scores
ORDER BY monetary_score DESC, frequency_score DESC, recency_score DESC
LIMIT 10;