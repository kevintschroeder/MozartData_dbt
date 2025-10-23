# load_data.py
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# Validate credentials
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE]):
    raise ValueError("❌ Missing Snowflake credentials in environment variables.")

def write_to_snowflake(data: pd.DataFrame):
    """
    Writes a pandas DataFrame to Snowflake table defined in .env
    """
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cursor = conn.cursor()

    # Optional: create schema if it doesn't exist
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")

    # Optional: truncate table before inserting
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")

    # Write data row by row
    for index, row in data.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (id, external_id, total_amount, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (row['id'], row['external_id'], row['total_amount'], row['created_at'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(data)} rows into {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")

# Optional: run as script for testing
if __name__ == "__main__":
    # Example test data
    test_df = pd.DataFrame([{
        "id": 1,
        "external_id": "ABC123",
        "total_amount": 100.50,
        "created_at": "2025-10-22"
    }])
    write_to_snowflake(test_df)
