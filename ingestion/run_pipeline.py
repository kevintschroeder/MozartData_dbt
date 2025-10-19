# run_pipeline.py
import login
import fetch_data
import load_data

def run_pipeline():
    # Step 1: get the token and org unit ID
    token, rouid = login.get_token()

    # Step 2: fetch data from API
    rows = fetch_data.fetch(token, rouid)

    # Step 3: load into Snowflake
    load_data.load_to_snowflake(rows)

if __name__ == "__main__":
    run_pipeline()
