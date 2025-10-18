# load_data.py
import pandas as pd
import snowflake.connector

def write_to_snowflake(data, config):
    """
    Writes a pandas DataFrame to Snowflake (Mozart warehouse).
    data: pandas DataFrame
    config: dictionary with Snowflake credentials
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

    # Optional: drop table if you want to overwrite
    # cursor.execute(f"DROP TABLE IF EXISTS {config['schema']}.{config['table']}")

    # Write data to Snowflake
    # This example uses pandas to generate INSERT statements
    for index, row in data.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {config['schema']}.{config['table']} (id, name, value, updated_at)
            VALUES (%s, %s, %s, %s)
            """,
            (row['id'], row['name'], row['value'], row['updated_at'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(data)} rows into {config['schema']}.{config['table']}")
