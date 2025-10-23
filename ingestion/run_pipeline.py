# run_pipeline.py
import pandas as pd
from login import get_token
from fetch_data import fetch
from load_data import write_to_snowflake


def main():
    """
    End-to-end ETL pipeline:
      1. Authenticate to Beanworks API
      2. Fetch invoice data
      3. Load into Snowflake (Mozart Data)
    """

    print("🔐 Logging into Beanworks...")
    access_token, root_org_unit_id = get_token()

    print("📥 Fetching data from Beanworks API...")
    rows = fetch(access_token, root_org_unit_id)

    if not rows:
        print("⚠️ No data returned from API.")
        return

    # Convert list of dicts into DataFrame
    df = pd.DataFrame(rows)
    print(f"✅ Retrieved {len(df)} rows")

    print("🏗️ Preparing to load into Snowflake...")

    # Snowflake (Mozart Data) connection settings
    snowflake_config = {
        "user": "KEVIN_ADMIN",              
        "password": os.getenv("SNOWFLAKE_PASSWORD"),      
        "account": "MOZARTDATA-MOZARTNDOA",        
        "warehouse": "PROD_CANDELARENEWABLES_DWH",
        "database": "PROD_CANDELARENEWABLES_DWH",
        "schema": "API_INGEST",
        "table": "RAW_DATA"
    }

    print("🚀 Loading data into Snowflake...")
    write_to_snowflake(df, snowflake_config)

    print("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    main()
