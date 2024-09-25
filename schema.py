# Define the schema for the vehicle data
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