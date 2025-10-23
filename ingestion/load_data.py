# load_data.py
import os
import snowflake.connector


def get_snowflake_conn_from_env():
    """
    Connect to Snowflake using environment variables.
    These should be stored as secrets in GitHub Actions or environment variables locally.
    """
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORMING"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "PROD_CANDELARENEWABLES_DWH"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "API_INGEST")
    )


def load_to_snowflake(rows):
    """
    Inserts API data into the Snowflake table api_ingest.raw_data.

    Args:
        rows (list of dict): Each dict represents one record from the API.
            Expected keys: id, external_id, total_amount, created_at
    """

    if not rows:
        print("No rows to insert — skipping Snowflake load.")
        return

    conn = get_snowflake_conn_from_env()
    cur = conn.cursor()

    try:
        # Create schema if needed
        cur.execute("CREATE SCHEMA IF NOT EXISTS api_ingest")

        # Create table if needed (optional)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api_ingest.raw_data (
                id STRING,
                external_id STRING,
                total_amount FLOAT,
                created_at TIMESTAMP_NTZ
            )
        """)

        # Insert rows
        insert_query = """
            INSERT INTO api_ingest.raw_data (id, external_id, total_amount, created_at)
            VALUES (%(id)s, %(external_id)s, %(total_amount)s, %(created_at)s)
        """
        cur.executemany(insert_query, rows)

        conn.commit()
        print(f"✅ Successfully inserted {len(rows)} rows into api_ingest.raw_data")

    except Exception as e:
        print("❌ Error loading data to Snowflake:", e)
        conn.rollback()

    finally:
        cur.close()
        conn.close()


# Optional test — lets you run this file standalone
if __name__ == "__main__":
    # Example test row (replace with real data)
    sample_rows = [
        {"id": "123", "external_id": "ABC123", "total_amount": 100.50, "created_at": "2025-10-22T00:00:00Z"}
    ]
    load_to_snowflake(sample_rows)
