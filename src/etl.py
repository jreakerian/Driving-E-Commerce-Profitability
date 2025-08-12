import os
import pandas as pd
from sqlalchemy import create_engine
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

def load_data(file_mapping, data_path):
    """Loads all CSV files into a dictionary of pandas DataFrames."""
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
    """Converts all DataFrame column names to lowercase snake_case."""
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    return df


def correct_data_types(dataframes):
    """Corrects data types for specific columns across all DataFrames."""
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


def load_to_db(df, table_name, engine):
    """
    Loads a DataFrame into a database table using the SQLAlchemy engine.
    NOTE: This function now expects the 'engine' object again.
    """
    try:
        df.to_sql(
            name=table_name,
            con=engine,  # Pass the engine object directly
            if_exists='replace',
            index=False,
            chunksize=10000,
            method='multi'
        )
        logging.info(f"✅ Successfully loaded data into '{table_name}'. Rows: {len(df)}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to load data into '{table_name}'. Error: {e}")
        return False


# --- Main ETL Logic ---

def main():
    """Main ETL function to orchestrate the entire pipeline."""
    logging.info("🚀 Starting ETL process...")

    # --- 1. EXTRACTION ---
    all_data = load_data(FILE_NAMES, DATA_DIR)
    if not all_data:
        logging.error("ETL process halted due to file loading errors.")
        return

    # --- 2. TRANSFORMATION ---
    logging.info("🔄 Starting data transformation...")
    for name, df in all_data.items():
        all_data[name] = clean_column_names(df)
    all_data = correct_data_types(all_data)

    products_df = all_data['products']
    category_translation_df = all_data['category_translation']
    products_df = pd.merge(products_df, category_translation_df, on='product_category_name', how='left')
    if 'product_category_name_english' in products_df.columns:
        products_df['product_category_name'] = products_df['product_category_name_english'].fillna(
            products_df['product_category_name'])
        products_df.drop(columns=['product_category_name_english'], inplace=True)
    all_data['products'] = products_df
    logging.info("Translated product categories to English.")

    # Create Dimension Tables
    dim_customers = all_data['customers'][
        ['customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()
    dim_products = all_data['products'][
        ['product_id', 'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm',
         'product_width_cm']].drop_duplicates(subset=['product_id'])
    dim_sellers = all_data['sellers'][
        ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']].drop_duplicates()
    dim_geolocation = all_data['geolocation'].drop_duplicates(subset=['geolocation_zip_code_prefix'])

    # Create Fact Table
    fact_order_items = pd.merge(all_data['orders'], all_data['customers'], on='customer_id')
    fact_order_items = pd.merge(fact_order_items, all_data['order_items'], on='order_id')
    payments_agg = all_data['payments'].groupby('order_id').agg(payment_value=('payment_value', 'sum'),
                                                                payment_installments=('payment_installments', 'max'),
                                                                payment_type=('payment_type', lambda x: x.mode()[
                                                                    0] if not x.empty else None)).reset_index()
    fact_order_items = pd.merge(fact_order_items, payments_agg, on='order_id', how='left')
    reviews_agg = all_data['reviews'].groupby('order_id').agg(review_score=('review_score', 'mean')).reset_index()
    fact_order_items = pd.merge(fact_order_items, reviews_agg, on='order_id', how='left')
    fact_order_items = fact_order_items[[
        'order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_unique_id', 'order_status',
        'order_purchase_timestamp', 'price', 'freight_value', 'payment_value', 'payment_installments',
        'payment_type', 'review_score'
    ]].copy()
    fact_order_items['review_score'] = fact_order_items['review_score'].fillna(0)
    fact_order_items['payment_value'] = fact_order_items['payment_value'].fillna(0)
    logging.info("Data transformation complete. Fact and dimension tables are ready.")

    # --- 3. LOADING ---
    logging.info("🚛 Starting data loading into Redshift...")
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        logging.error("Missing database credentials in environment variables. Halting.")
        for var in ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']:
            if not os.getenv(var):
                logging.error(f"--> Missing: {var}")
        return

    connection_string = f"redshift+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    try:
        engine = create_engine(connection_string)

        # First, test the connection to fail fast if credentials are wrong
        with engine.connect() as connection:
            logging.info("✅ Database connection test successful!")

        # Now, load the tables by passing the engine directly
        tables_to_load = {
            'dim_customers': dim_customers,
            'dim_products': dim_products,
            'dim_sellers': dim_sellers,
            'dim_geolocation': dim_geolocation,
            'fact_order_items': fact_order_items
        }

        all_successful = True
        for name, df in tables_to_load.items():
            if not load_to_db(df, name, engine):
                all_successful = False

        if all_successful:
            logging.info("🎉 ETL process completed successfully! All tables loaded.")
        else:
            logging.warning("⚠️ ETL process completed with one or more failures. Please check logs.")

    except Exception as e:
        logging.error(f"❌ Database connection or loading failed. Error: {e}")
        logging.error("Please check your database credentials, VPN/network access, and Redshift cluster status.")
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("Database engine disposed.")


if __name__ == '__main__':
    main()