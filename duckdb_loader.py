import csv
import duckdb
import time

"""
    A class to handle creating DuckDB tables, loading CSV data, and executing SQL queries.
"""
class DuckDBTableLoader:
    def __init__(self, file_path: str, table_name: str, schema: dict):
        self.__file_path = file_path  # Private file path
        self.__table_name = table_name  # Private table name
        self.__schema = schema  # Private table schema
        self.__connection = duckdb.connect(database=':memory:', read_only=False)  # Initialize DuckDB connection
        self.__load_spatial_extension()

    def __load_spatial_extension(self):
        """Install and load the spatial extension in DuckDB."""
        self.__connection.execute("INSTALL spatial")
        self.__connection.execute("LOAD spatial")

    def set_file_path(self, file_path: str):
        """Sets a new file path for the CSV data."""
        self.__file_path = file_path

    def create_table_from_schema(self):
        """Creates a table in DuckDB based on the provided schema."""
        schema_definition = ", ".join([f'"{col}" {dtype}' for col, dtype in self.__schema.items()])
        create_query = f'CREATE TABLE IF NOT EXISTS {self.__table_name} ({schema_definition})'
        self.__connection.execute(create_query)

    def load_data_from_csv1(self):
        # Load CSV directly into DuckDB using built-in function
        try:
            start_csv_time = time.time()
            # Using DuckDB's built-in function to read CSV directly into the table
            self.__connection.execute(f"""
                COPY "{self.__table_name}" FROM '{self.__file_path}' (FORMAT CSV, HEADER TRUE)
            """)
            end_csv_time = time.time()

            print(f"Time spent in CSV iteration: {end_csv_time - start_csv_time:.2f} seconds")
        except Exception as e:
            print(f"Error during CSV loading: {e}")

    def load_data_from_csv(self):
        start_csv_time = time.time()
        with open(self.__file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            data = []

            for row in csv_reader:
                # Extract values based on schema, excluding 'Vehicle Location'
                values = [self.__convert_value(row[col], self.__schema[col]) for col in self.__schema if col != 'Vehicle Location']
                
                # Handle the Vehicle Location as two separate columns: longitude and latitude
                if 'Vehicle Location' in row and row['Vehicle Location']:
                    lon, lat = map(float, row['Vehicle Location'].strip('POINT ()').split())
                    values.extend([lat, lon])  # Append latitude and longitude
                else:
                    values.extend([None, None])  # For rows with missing location

                data.append(values)

            end_csv_time = time.time()
            print(f"Time spent in CSV iteration: {end_csv_time - start_csv_time:.2f} seconds")

            # Prepare the insert query
            insert_query = f"""
                INSERT INTO "{self.__table_name}" (
                    {', '.join([f'"{col}"' for col in self.__schema if col != 'Vehicle Location'])}, 
                    "Vehicle Location"
                ) VALUES """
            
            # Create a string of values for all rows
            values_list = []
            for vals in data:
                # handle single quote in the string
                vals_str = ', '.join(
                    ['NULL' if v is None else f"'{str(v).replace('\'', '\'\'')}'" if isinstance(v, str) else str(v) for v in vals[:-2]]
                )

                # Handle the POINT location with condition to avoid None
                if vals[-2] is None or vals[-1] is None:
                    location_str = "NULL"  # Use NULL for missing coordinates
                else:
                    location_str = f"ST_Point({vals[-2]}, {vals[-1]})"

                values_list.append(f"({vals_str}, {location_str})")

            # Join all value strings with commas
            insert_query += ', '.join(values_list)

            end_csv_time = time.time()

            print(f"Time spent in CSV iteration: {end_csv_time - start_csv_time:.2f} seconds")

            # self.__connection.execute("BEGIN TRANSACTION;")
            # Execute insert in batches for better performance
            self.__connection.execute(insert_query)
            # self.__connection.execute("COMMIT;")

            end_db_insert_time = time.time()
            print(f"Time spent in DB insert: {end_db_insert_time - end_csv_time:.2f} seconds")


    def __convert_value(self, value, dtype):
        """
        Handles missing or empty values and returns None for those cases.
        """
        if value == '' or value is None:
            # Handle empty strings or None values
            return None

        return value

    def reload_data(self):
        """Clears the table and reloads data from the CSV file."""
        self.__connection.execute(f"DELETE FROM {self.__table_name}")
        self.load_data_from_csv()

    def execute_sql(self, query: str):
        """Executes an SQL query on the DuckDB connection and returns the result."""
        result = self.__connection.execute(query).fetchall()
        return result
