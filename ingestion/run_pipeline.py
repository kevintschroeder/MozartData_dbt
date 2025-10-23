# run_pipeline.py
import os
import pandas as pd
from dotenv import load_dotenv
from login import get_token
from fetch_data import fetch
from load_data import write_to_snowflake

def main():
    # Load environment variables from .env
    load_dotenv()

    snowflake_config = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "table": os.getenv("SNOWFLAKE_TABLE"),
    }

    # Ensure all required credentials are present
    if not all(snowflake_config.values()):
        raise ValueError("❌ Missing Snowflake credentials in environment variables.")

    print("🔐 Logging into Beanworks...")
    access_token, root_org_unit_id = get_token()
    print("✅ Successfully authenticated with Beanworks API")

    print("📥 Fetching data from Beanworks API...")
    rows = fetch(access_token, root_org_unit_id)
    print(f"✅ Retrieved {len(rows)} rows")

    print("🏗️ Preparing to load into Snowflake...")
    df = pd.DataFrame(rows)

    print("🚀 Loading data into Snowflake...")
    write_to_snowflake(df, snowflake_config)
    print("🎉 Pipeline completed successfully!")

if __name__ == "__main__":
    main()
