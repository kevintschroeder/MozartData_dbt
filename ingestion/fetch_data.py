# fetch_data.py
import requests
import json

def fetch(access_token, root_org_unit_id):
    """
    Fetch data from Beanworks API using access token and root org unit ID.
    
    Returns:
        rows (list of dicts): each dict represents a row to insert into Snowflake.
    """
    
    # GraphQL endpoint
    url_api = 'https://beanworks.ca/api/graphql/'
    url_with_rouid = f'{url_api}?rouid={root_org_unit_id}'
    
    # GraphQL query (modify this based on the data you need)
    query = """
    {
        invoices {
            edges {
                node {
                    id
                    externalId
                    totalAmount
                    createdAt
                }
            }
        }
    }
    """
    
    # Headers for API request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Http-Authorization': f'Bearer {access_token}'
    }
    
    # Prepare POST request data
    data = {'query': query}
    
    # Make the API request
    response = requests.post(url_with_rouid, headers=headers, data=data)
    
    # Check if request was successful
    if response.status_code != 200:
        print(f"❌ GraphQL query failed with status {response.status_code}")
        print(response.text)
        raise Exception("Stopping because of API error.")
    
    # Parse JSON response
    result = response.json()

    # 🚨 Debug: print the full response to see what we get
    print("🔍 Full API response:")
    print(json.dumps(r
