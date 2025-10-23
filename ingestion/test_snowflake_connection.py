# test_snowflake_connection.py
import os
import snowflake.connector
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Read credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    # Test query: list tables in schema
    cursor.execute(f"SHOW TABLES IN SCHEMA {SNOWFLAKE_SCHEMA};")
    tables = cursor.fetchall()
    print(f"✅ Connected successfully! Tables in schema '{SNOWFLAKE_SCHEMA}':")
    for table in tables:
        print(" -", table[1])  # table[1] is the table name

except Exception as e:
    print("❌ Connection failed:", e)

finally:
    cursor.close()
    conn.close()
