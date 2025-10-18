# run_pipeline.py
import login
import fetch_data
import load_data
import pandas as pd

# Step 1: Get API token
token = login.get_token()

# Step 2: Fetch data from API
api_json = fetch_data.fetch(token)  # Should return a list of dicts

# Step 3: Convert to pandas DataFrame
df = pd.DataFrame(api_json)

# Step 4: Snowflake/Mozart connection info
snowflake_config = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "api_ingest",
    "table": "raw_data"
}

# Step 5: Write data to warehouse
load_data.write_to_snowflake(df, snowflake_config)
