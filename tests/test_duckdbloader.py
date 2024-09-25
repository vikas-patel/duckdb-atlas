from database_loader.duckdb_loader import DuckDBTableLoader
import pytest
import os
import tempfile
import pandas as pd
from main import VEHICLE_SCHEMA

# Sample data for CSV
CSV_DATA = [
    ['5YJ3E1EB4L', 'Yakima', 'Yakima', 'WA', '98908', 2020, 'TESLA', 'MODEL 3', 'Battery Electric Vehicle (BEV)', 'Clean Alternative Fuel Vehicle Eligible', 322, 0, 14, 127175366, 'POINT(46.58514 -120.56916)', 'PACIFICORP', 53077000904],
    ['5YJ3E1EA7K', 'San Diego', 'San Diego', 'CA', '92101', 2019, 'TESLA', 'MODEL 3', 'Battery Electric Vehicle (BEV)', 'Clean Alternative Fuel Vehicle Eligible', 220, 0, None, 266614659, 'POINT(32.71568 -117.16171)', None, 6073005102],
]

@pytest.fixture
def create_temp_csv():
    # Create a temporary CSV file for testing
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame(CSV_DATA, columns=VEHICLE_SCHEMA.keys())
    df.to_csv(tmp_file.name, index=False)
    yield tmp_file.name
    # Cleanup
    os.remove(tmp_file.name)

@pytest.fixture
def db_loader(create_temp_csv):
    # Initialize the DuckDBTableLoader with the temp CSV and schema
    loader = DuckDBTableLoader(file_path=create_temp_csv, table_name="ev_population", schema=VEHICLE_SCHEMA)
    loader.create_table_from_schema()
    return loader

def test_duckdb_loader_init(db_loader):
    # Test the initialization of DuckDBTableLoader
    assert db_loader is not None
    assert db_loader._DuckDBTableLoader__connection is not None
    assert db_loader._DuckDBTableLoader__table_name == "ev_population"

def test_load_csv_data(db_loader):
    # Test loading the CSV data into the DuckDB table
    db_loader.load_data_from_csv()

    # Query to check the number of records loaded
    count_query = f"SELECT COUNT(*) FROM ev_population"
    result = db_loader.execute_sql(count_query)
    assert result[0][0] == len(CSV_DATA)  # Ensure all records are loaded

    # Verify that specific data exists
    query = f"SELECT * FROM ev_population WHERE City = 'Yakima'"
    result = db_loader.execute_sql(query)
    assert len(result) == 1
    assert result[0][1] == 'Yakima'  # Verify that the city matches
