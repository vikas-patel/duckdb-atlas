# Queries related to electric cars
TABLE_NAME = "ev_population"  # Replace with a dynamic variable if needed
OUTPUT_DIR = "data/output"  # Output directory for parquet files

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
(FORMAT 'parquet', PARTITION_BY 'Model Year', OVERWRITE TRUE);
"""
