# run_pipeline.py
import os
import pandas as pd
from dotenv import load_dotenv
from login import get_token
from fetch_data import fetch
from load_data import write_to_snowflake

def main():
    # 🔐 Load environment variables
    load_dotenv()  # Loads .env file in current folder

    snowflake_config = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "table": os.getenv("SNOWFLAKE_TABLE"),
    }

    # Ensure all credentials are present
    missing = [k for k, v in snowflake_config.items() if not v]
    if missing:
        raise ValueError(f"❌ Missing Snowflake credentials: {missing}")

    # 🔐 Authenticate with Beanworks
    print("🔐 Logging into Beanworks...")
    access_token, root_org_unit_id = get_token()
    print("✅ Successfully authenticated with Beanworks API")

    # 📥 Fetch data from Beanworks API
    print("📥 Fetching data from Beanworks API...")
    rows = fetch(access_token, root_org_unit_id)
    print(f"✅ Retrieved {len(rows)} rows")

    # 🏗️ Convert to DataFrame
    df = pd.DataFrame(rows)

    # 🚀 Load into Snowflake
    print("🚀 Loading data into Snowflake...")
    write_to_snowflake(df, snowflake_config)
    print("✅ Data load complete!")

if __name__ == "__main__":
    main()
