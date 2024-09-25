import os
from typing import Dict
import logging

from database_loader.duckdb_loader import DuckDBTableLoader
from database_loader.table_loader_interface import TableLoaderInterface
from queries import COUNT_CARS_QUERY, MOST_POPULAR_VEHICLES_QUERY, OUTPUT_DIR, PARQUET_WRITE_QUERY, TABLE_NAME, TOP_VEHICLES_QUERY
from schema import VEHICLE_SCHEMA

# Constants
FILE_PATH = "data/Electric_Vehicle_Population_Data.csv"

logging.basicConfig(level=logging.INFO)

def main(loader: TableLoaderInterface):
    # setup
    try:
        loader.create_table_from_schema()
        logging.info("Table created successfully.")
        loader.fast_load_data_from_csv()
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during table creation or data loading: {e}")
    
    # create parquet output dir if missing
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logging.info(f"Created output directory: {OUTPUT_DIR}")

    # execute queries
    try:
        queries = [
            (COUNT_CARS_QUERY, "Electric cars count by city:"),
            (TOP_VEHICLES_QUERY, "Top 3 most popular electric vehicles:"),
            (MOST_POPULAR_VEHICLES_QUERY, "Most popular electric vehicle in each postal code:")
        ]

        # Iterate through the queries and execute them
        for query, description in queries:
            result = loader.execute_sql(query)
            logging.info(f"{description} executed successfully.")
            print(f"{description}\n{result}\n")

        loader.execute_sql(PARQUET_WRITE_QUERY)
        print(f"Data successfully written to {OUTPUT_DIR} partitioned by Model Year.")

    except Exception as e:
        logging.error(f"Error executing query: {e}")

if __name__ == "__main__":
    # Instantiate the DuckDBTableLoader or PostgresTableLoader or future DB Server using the interface
    loader: TableLoaderInterface = DuckDBTableLoader(
        file_path=FILE_PATH,
        table_name=TABLE_NAME,
        schema=VEHICLE_SCHEMA
    )
    
    # Call the main function with the instantiated loader
    main(loader)
