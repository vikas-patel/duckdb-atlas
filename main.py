from typing import Dict
import logging

from duckdb_loader import DuckDBTableLoader

# Constants
FILE_PATH = "data/Electric_Vehicle_Population_Data.csv"
TABLE_NAME = "ev_population"
OUTPUT_DIR = "."

VEHICLE_SCHEMA = {
    "VIN (1-10)": "VARCHAR(10)",  # max length 10
    "County": "VARCHAR(50)",       # county name should be short
    "City": "VARCHAR(50)",         # city names should be short
    "State": "VARCHAR(2)",         # 2 digit state abbreviations
    "Postal Code": "VARCHAR(10)",  # Alphanumeric values, never seen above 10
    "Model Year": "SMALLINT",      # max 4 digit, using SMALLINT
    "Make": "VARCHAR(50)",
    "Model": "VARCHAR(50)",
    "Electric Vehicle Type": "VARCHAR(50)",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR(100)",
    "Electric Range": "SMALLINT",  # Small range for electric range in miles
    "Base MSRP": "BIGINT",         # Large range for MSRP
    "Legislative District": "SMALLINT",  # Small range for legislative districts
    "DOL Vehicle ID": "BIGINT",    # Large range for DOL vehicle IDs
    "Vehicle Location": "POINT_2D",     # Geographic location in POINT format
    "Electric Utility": "VARCHAR(50)",
    "2020 Census Tract": "BIGINT"   # Large range for census tract IDs
}

# Query for cars by city
COUNT_CARS_QUERY = f"""
    SELECT City, COUNT(*) AS electric_car_count
    FROM {TABLE_NAME}
    GROUP BY City
    ORDER BY electric_car_count DESC;
"""

# Query for top 3 electric vehicles
TOP_VEHICLES_QUERY = f"""
    SELECT
        "Make",
        "Model",
        COUNT(*) AS vehicle_count
    FROM {TABLE_NAME}
    GROUP BY
        "Make", "Model"
    ORDER BY
        vehicle_count DESC
    LIMIT 3;
"""

# Query for most popular vehicles by postal code
MOST_POPULAR_VEHICLES_QUERY = f"""
    WITH RankedVehicles AS (
        SELECT
            "Postal Code",
            "Make",
            "Model",
            COUNT(*) AS vehicle_count,
            ROW_NUMBER() OVER (PARTITION BY "Postal Code" ORDER BY COUNT(*) DESC) AS rank
        FROM {TABLE_NAME}
        GROUP BY
            "Postal Code", "Make", "Model"
    )
    SELECT
        "Postal Code",
        "Make",
        "Model",
        vehicle_count
    FROM RankedVehicles
    WHERE rank = 1;
"""

# SQL query to count electric cars by model year
COUNT_BY_YEAR_QUERY = f"""
SELECT "Model Year", COUNT(*) as electric_car_count
FROM {TABLE_NAME}
GROUP BY "Model Year"
"""

# write the query result to Parquet files partitioned by "Model Year"
PARQUET_WRITE_QUERY = f"""
COPY (
    {COUNT_BY_YEAR_QUERY}
) TO '{OUTPUT_DIR}/electric_car_count.parquet'
(FORMAT 'parquet', PARTITION_BY 'Model Year');
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
        loader.fast_load_data_from_csv()
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during table creation or data loading: {e}")

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
    main()
