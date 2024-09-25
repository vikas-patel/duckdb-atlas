import csv
from database_loader.table_loader_interface import TableLoaderInterface
import duckdb
import pandas as pd

"""
    A class to handle creating DuckDB tables, loading CSV data, and executing SQL queries.
"""
class DuckDBTableLoader(TableLoaderInterface):
    def __init__(self, file_path: str, table_name: str, schema: dict):
        self.__file_path = file_path
        self.__table_name = table_name
        self.__schema = schema
        self.__connection = duckdb.connect(database=':memory:', read_only=False)
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

    def fast_load_data_from_csv(self):
        # Load CSV into a pandas DataFrame
        df = pd.read_csv(self.__file_path)

        # Convert 'Vehicle Location' into latitude and longitude columns
        if 'Vehicle Location' in df.columns:
            df[['lon', 'lat']] = df['Vehicle Location'].str.extract(r'POINT \(([\d\.\-]+) ([\d\.\-]+)\)')
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        else:
            df['lat'], df['lon'] = None, None

        # Drop the original 'Vehicle Location' column
        df = df.drop(columns=['Vehicle Location'])

        # Create a temporary table schema based on the schema
        temp_table_name = f"{self.__table_name}_temp"
        columns_def = ', '.join([f'"{col}" {dtype}' for col, dtype in self.__schema.items() if col != 'Vehicle Location'])
        columns_def += ', "lat" DOUBLE, "lon" DOUBLE'

        create_temp_table_query = f"""
            CREATE TEMPORARY TABLE {temp_table_name} ({columns_def});
        """
        self.__connection.execute(create_temp_table_query)

        self.__connection.register('temp_df', df)

        # Insert data from the pandas DataFrame into the temporary table
        insert_into_temp_table_query = f"""
            INSERT INTO {temp_table_name}
            SELECT * FROM temp_df;
        """
        self.__connection.execute(insert_into_temp_table_query)

        # Insert data from the temporary table into the main table
        insert_query = f"""
            INSERT INTO "{self.__table_name}" (
                {', '.join([f'"{col}"' for col in self.__schema if col != 'Vehicle Location'])},
                "Vehicle Location"
            )
            SELECT {', '.join([f'"{col}"' for col in self.__schema if col != 'Vehicle Location'])}, 
                ST_Point(lat, lon)
            FROM {temp_table_name};
        """
        self.__connection.execute(insert_query)

    def load_data_from_csv(self):
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
            self.__connection.execute(insert_query)


    def __convert_value(self, value, dtype):
        """
        Handles missing or empty values and returns None for those cases.
        """
        if value == '' or value is None:
            # Handle empty strings or None values
            return None

        return value

    def execute_sql(self, query: str):
        """Executes an SQL query on the DuckDB connection and returns the result."""
        result = self.__connection.execute(query).fetchall()
        return result
