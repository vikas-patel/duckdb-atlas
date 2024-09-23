from typing import Dict
import logging

from duckdb_loader import DuckDBTableLoader

# Constants
FILE_PATH = "data/Electric_Vehicle_Population_Data.csv"
TABLE_NAME = "ev_population"

VEHICLE_SCHEMA = {
    "VIN (1-10)": "VARCHAR(10)",  # Fixed length for VIN
    "County": "VARCHAR(50)",       # county name should be short
    "City": "VARCHAR(50)",         # city names should be short
    "State": "VARCHAR(2)",         # Fixed length for state abbreviations
    "Postal Code": "VARCHAR(10)",  # Alphanumeric values, never seen above 10
    "Model Year": "SMALLINT",      # Small range for year values, using SMALLINT
    "Make": "VARCHAR(50)",
    "Model": "VARCHAR(50)",
    "Electric Vehicle Type": "VARCHAR(50)",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR(100)",
    "Electric Range": "SMALLINT",  # Small range for electric range in miles
    "Base MSRP": "BIGINT",         # Large range for MSRP
    "Legislative District": "SMALLINT",  # Small range for legislative districts
    "DOL Vehicle ID": "BIGINT",    # Large range for DOL vehicle IDs
    "Vehicle Location": "VARCHAR",     # Geographic location in POINT format
    "Electric Utility": "VARCHAR(50)",
    "2020 Census Tract": "BIGINT"   # Large range for census tract IDs
}

COUNT_CARS_QUERY = f"""
    SELECT City, COUNT(*) AS electric_car_count
    FROM {TABLE_NAME}
    GROUP BY City
    ORDER BY electric_car_count DESC;
"""

logging.basicConfig(level=logging.INFO)

def main():
    # Instantiate the DuckDBTableLoader
    loader = DuckDBTableLoader(
        file_path=FILE_PATH,
        table_name=TABLE_NAME,
        schema=VEHICLE_SCHEMA
    )
    # setup
    try:
        loader.create_table_from_schema()
        logging.info("Table created successfully.")
        loader.load_data_from_csv()
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during table creation or data loading: {e}")

    # execute queries
    try:
        # result = loader.execute_sql(COUNT_CARS_QUERY)
        logging.info("Query executed successfully.")
        # print(result)
    except Exception as e:
        logging.error(f"Error executing query: {e}")

if __name__ == "__main__":
    main()
