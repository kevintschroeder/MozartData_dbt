# load_data.py
import os
import pandas as pd
import snowflake.connector

def write_to_snowflake(data: pd.DataFrame, config: dict):
    """
    Writes a pandas DataFrame to Snowflake (Mozart warehouse).

    Args:
        data (pd.DataFrame): DataFrame containing rows to insert.
        config (dict): Dictionary containing Snowflake credentials and target table info.
            Expected keys: user, password, account, warehouse, database, schema, table
    """
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

    # Optional: create table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config['schema']}.{config['table']} (
            id STRING,
            external_id STRING,
            total_amount FLOAT,
            created_at TIMESTAMP
        )
    """)

    # Insert rows using parameterized queries
    insert_sql = f"""
        INSERT INTO {config['schema']}.{config['table']} (id, external_id, total_amount, created_at)
        VALUES (%s, %s, %s, %s)
    """

    for _, row in data.iterrows():
        cursor.execute(
            insert_sql,
            (row['id'], row['external_id'], row['total_amount'], row['created_at'])
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {len(data)} rows into {config['schema']}.{config['table']}")
