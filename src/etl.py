import os
import pandas as pd
import psycopg2
import boto3  # AWS SDK for Python
import logging
from dotenv import load_dotenv

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Environment Variables ---
load_dotenv()

# --- Database Credentials & File Paths ---
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5439')
DB_NAME = os.getenv('DB_NAME')

# --- NEW: S3 and IAM Configuration ---
S3_BUCKET = os.getenv('S3_BUCKET')
REDSHIFT_IAM_ROLE_ARN = os.getenv('REDSHIFT_IAM_ROLE_ARN')

DATA_DIR = r'..\data'
FILE_NAMES = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'category_translation': 'product_category_name_translation.csv'
}


# --- Helper Functions ---
# load_data, clean_column_names, and correct_data_types remain the same.
def load_data(file_mapping, data_path):
    dataframes = {}
    for name, filename in file_mapping.items():
        path = os.path.join(data_path, filename)
        try:
            dataframes[name] = pd.read_csv(path)
            logging.info(f"Successfully loaded {filename}")
        except FileNotFoundError:
            logging.error(f"File not found: {path}. Please check the path and file name.")
            return None
    return dataframes


def clean_column_names(df):
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    return df


def correct_data_types(dataframes):
    timestamp_cols = {
        'orders': ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
                   'order_delivered_customer_date', 'order_estimated_delivery_date'],
        'reviews': ['review_creation_date', 'review_answer_timestamp']
    }
    for df_name, cols in timestamp_cols.items():
        if df_name in dataframes:
            for col in cols:
                if col in dataframes[df_name].columns:
                    dataframes[df_name][col] = pd.to_datetime(dataframes[df_name][col], errors='coerce')
    logging.info("Corrected timestamp data types.")
    return dataframes


def get_redshift_type(dtype):
    if "int64" in str(dtype):
        return "BIGINT"
    elif "float64" in str(dtype):
        return "FLOAT8"
    elif "datetime" in str(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR(512)"  # Increased size for safety


def load_to_db(df, table_name, conn, s3_client):
    """
    Loads a DataFrame to Redshift by staging it in an S3 bucket.
    """
    # Define a temporary local file path and an S3 object key
    local_path = f"{table_name}.csv"
    s3_key = f"etl-staging/{table_name}/{local_path}"

    try:
        # 1. Save DataFrame to a local CSV file
        df.to_csv(local_path, index=False, header=False, sep='|', na_rep='NULL')
        logging.info(f"Saved {table_name} to local file: {local_path}")

        # 2. Upload the local file to S3
        s3_client.upload_file(local_path, S3_BUCKET, s3_key)
        logging.info(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")

        # 3. Execute Redshift commands
        with conn.cursor() as cursor:
            # Instead of dropping and creating, we TRUNCATE the existing table.
            # TRUNCATE is a fast way to delete all rows from a table.
            logging.info(f"Truncating table '{table_name}' before loading...")
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            logging.info(f"Table '{table_name}' truncated.")

            # Execute the COPY command from S3
            s3_path = f"s3://{S3_BUCKET}/{s3_key}"
            copy_sql = f"""
                COPY {table_name}
                FROM '{s3_path}'
                IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
                DELIMITER '|'
                NULL AS 'NULL'
                TRUNCATECOLUMNS
                CSV;
            """
            cursor.execute(copy_sql)
            logging.info(f"Executed COPY command for '{table_name}' from S3.")

        conn.commit()
        logging.info(f"✅ Successfully loaded data into '{table_name}'. Rows: {len(df)}")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Failed to load data into '{table_name}'. Error: {e}")
        return False
    finally:
        # 4. Clean up the local file
        if os.path.exists(local_path):
            os.remove(local_path)
            logging.info(f"Removed temporary local file: {local_path}")


# --- Main ETL Logic ---
def main():
    logging.info("🚀 Starting ETL process...")

    # --- 1. EXTRACTION & 2. TRANSFORMATION ---
    # (No changes here, logic is the same)
    all_data = load_data(FILE_NAMES, DATA_DIR)
    if not all_data: return

    logging.info("🔄 Starting data transformation...")
    for name, df in all_data.items(): all_data[name] = clean_column_names(df)
    all_data = correct_data_types(all_data)

    products_df = pd.merge(all_data['products'], all_data['category_translation'], on='product_category_name',
                           how='left')
    if 'product_category_name_english' in products_df.columns:
        products_df['product_category_name'] = products_df['product_category_name_english'].fillna(
            products_df['product_category_name'])
        products_df.drop(columns=['product_category_name_english'], inplace=True)
    all_data['products'] = products_df

    dim_customers = all_data['customers'][
        ['customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()

    dim_products = all_data['products'][
        ['product_id', 'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm',
         'product_width_cm']].drop_duplicates(subset=['product_id'])
    # Define the columns that should be integers
    integer_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        # First, fill any potential missing values (NaNs) with 0
    for col in integer_cols:
        dim_products[col] = dim_products[col].fillna(0)
    # Now, safely cast the columns to integer type
    dim_products = dim_products.astype({col: 'int' for col in integer_cols})
    logging.info("Enforced integer types for dim_products physical dimension columns.")


    dim_sellers = all_data['sellers'][
        ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']].drop_duplicates()
    dim_geolocation = all_data['geolocation'][[
        'geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
    ]].drop_duplicates(subset=['geolocation_zip_code_prefix'])
    logging.info("Created and selected columns for dim_geolocation DataFrame.")# --- Add this code within the transformation section of your main() function ---

    # Create a new dimension table for orders to hold delivery timestamps
    dim_orders = all_data['orders'][[
        'order_id',
        'customer_id',
        'order_status',
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]].drop_duplicates(subset=['order_id'])
    logging.info("Created dim_orders DataFrame.")

    fact_order_items = pd.merge(pd.merge(all_data['orders'], all_data['customers'], on='customer_id'),
                                all_data['order_items'], on='order_id')
    payments_agg = all_data['payments'].groupby('order_id').agg(payment_value=('payment_value', 'sum'),
                                                                payment_installments=('payment_installments', 'max'),
                                                                payment_type=('payment_type', lambda x: x.mode()[
                                                                    0] if not x.empty else None)).reset_index()
    fact_order_items = pd.merge(fact_order_items, payments_agg, on='order_id', how='left')
    reviews_agg = all_data['reviews'].groupby('order_id').agg(review_score=('review_score', 'mean')).reset_index()
    fact_order_items = pd.merge(fact_order_items, reviews_agg, on='order_id', how='left')
    fact_order_items = fact_order_items[
        ['order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_unique_id',
         'order_purchase_timestamp', 'price', 'freight_value', 'payment_value', 'payment_installments', 'payment_type',
         'review_score']].copy()
    # First, fill nulls for all relevant columns
    fact_order_items['review_score'] = fact_order_items['review_score'].fillna(0)
    fact_order_items['payment_value'] = fact_order_items['payment_value'].fillna(0)
    fact_order_items['payment_installments'] = fact_order_items['payment_installments'].fillna(0)

    # Now, enforce the correct integer types for columns defined as SMALLINT in the DDL
    integer_cols = ['payment_installments', 'review_score']
    fact_order_items = fact_order_items.astype({col: 'int' for col in integer_cols})
    logging.info("Enforced integer types for fact_order_items columns.")
    logging.info("Data transformation complete.")

    # --- 3. LOADING ---
    conn = None
    try:
        logging.info("🚛 Connecting to Redshift and S3...")
        # Create S3 and Redshift clients
        s3_client = boto3.client('s3')
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logging.info("✅ Connections to Redshift and S3 successful!")

        tables_to_load = {
            'dim_customers': dim_customers,
            'dim_products': dim_products,
            'dim_sellers': dim_sellers,
            'dim_geolocation': dim_geolocation,
            'fact_order_items': fact_order_items,
            'dim_orders': dim_orders
        }

        # Load tables in an order that respects dependencies (dims before facts)
        all_successful = True
        for name, df in tables_to_load.items():
            if not load_to_db(df, name, conn, s3_client):
                all_successful = False
                break  # Stop if one table fails

        if all_successful:
            logging.info("🎉 ETL process completed successfully! All tables loaded.")
        else:
            logging.warning("⚠️ ETL process completed with one or more failures. Please check logs.")

    except Exception as e:
        logging.error(f"❌ A critical error occurred in the main process. Error: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


if __name__ == '__main__':
    main()