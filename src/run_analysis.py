# src/run_analysis.py

import os
import pandas as pd
import psycopg2  # Import the psycopg2 library
import logging
from dotenv import load_dotenv


# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# Database credentials from environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5439')
DB_NAME = os.getenv('DB_NAME')

# Path to the SQL queries file
SQL_FILE_PATH = 'sql/analysis_queries.sql'


def get_queries_from_file(filepath):
    """Reads SQL queries from a file and splits them into a list."""
    with open(filepath, 'r') as file:
        full_sql = file.read()
        # Split queries by semicolon and filter out empty statements
        queries = [q.strip() for q in full_sql.split(';') if q.strip()]
        return queries


def run_analysis():
    """Connects to the database using psycopg2, runs analysis queries, and prints results."""
    # **FIXED LINE**: Check if all necessary environment variables are set.
    credentials = {}
    if not all(credentials):
        logging.error(
            "Database credentials are not fully configured. Please set all required environment variables: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME.")
        return

    conn = None  # Initialize connection to None
    try:
        # Establish the connection using psycopg2.connect
        logging.info(f"Connecting to the database '{DB_NAME}' on host '{DB_HOST}'...")
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logging.info("Database connection successful.")

        # Read the queries from the SQL file
        try:
            analysis_queries = get_queries_from_file(SQL_FILE_PATH)
            logging.info(f"Successfully loaded {len(analysis_queries)} queries from {SQL_FILE_PATH}.")
        except FileNotFoundError:
            logging.error(f"SQL file not found at {SQL_FILE_PATH}. Please check the path.")
            return

        # Execute each query and display the results
        for i, query in enumerate(analysis_queries):
            logging.info(f"\n--- Executing Query #{i + 1} ---")
            print(f"Query: \n{query[:200]}...\n")

            try:
                # Use pandas.read_sql_query, passing the psycopg2 connection object directly
                result_df = pd.read_sql_query(query, conn)

                print("--- Results ---")
                print(result_df.to_string())
                print("\n" + "=" * 50 + "\n")

            except (Exception, psycopg2.Error) as e:
                logging.error(f"Failed to execute query #{i + 1}. Error: {e}")
                print("\n" + "=" * 50 + "\n")

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while connecting to Redshift: {error}")
    finally:
        # Close the connection if it was successfully established
        if conn is not None:
            conn.close()
            logging.info("Database connection closed.")


if __name__ == '__main__':
    run_analysis()