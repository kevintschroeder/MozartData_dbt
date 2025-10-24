# load_data.py
import os
import pandas as pd
import snowflake.connector

def write_to_snowflake(data: pd.DataFrame, config: dict):
    """
    Writes a pandas DataFrame to Snowflake (Mozart warehouse).

    Args:
        data (pd.DataFrame): Data to insert.
        config (dict): Dictionary with Snowflake credentials:
            user, password, account, warehouse, database, schema, table
    """

    # Ensure all required keys exist
    required_keys = ["user", "password", "account", "warehouse", "database", "schema", "table"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise ValueError(f"❌ Missing Snowflake credentials/config keys: {missing}")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )

    cursor = conn.cursor()

    # Optional: create schema if it doesn't exist
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config['schema']}")

    # Optional: drop table if overwriting
    # cursor.execute(f"DROP TABLE IF EXISTS {config['schema']}.{config['table']}")

    # Write data to Snowflake
    for index, row in data.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {config['schema']}.{config['table']} 
            (id, external_id, amount, created)
            VALUES (%s, %s, %s, %s)
            """,
            (row['id'], row['external_id'], row['amount'], row['created'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(data)} rows into {config['schema']}.{config['table']}")
