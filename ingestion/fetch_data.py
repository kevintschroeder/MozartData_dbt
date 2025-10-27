# fetch_data.py
import requests
import json
import pandas as pd

def fetch(access_token, root_org_unit_id):
    """
    Fetch data from Beanworks API using access token and root org unit ID.
    
    Returns:
        df (pandas DataFrame): each row represents a record to insert into Snowflake.
    """

    # GraphQL endpoint
    url_api = 'https://beanworks.ca/api/graphql/'
    url_with_rouid = f'{url_api}?rouid={root_org_unit_id}'

    # GraphQL query with a minimal 'where' argument (required by API)
    query = """
    {
      invoices(where: {
	updated: { _gt: "2025-10-15T00:00:00Z" }
      }) {
        edges {
          node {
            id
            number
	    externalId
            poNumber
	    status
	    paidStatus
	    alreadyPaidAmount
	    created
            total
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
        raise Exception(f"GraphQL query failed: {response.status_code}\n{response.text}")

    # Parse JSON response
    result = response.json()

    # Debug: show full response if 'data' key is missing
    if 'data' not in result:
        print("❌ Full API response:")
        print(json.dumps(result, indent=2))
        raise KeyError("'data' key missing in response — check your API query or token.")

    # Extract the data into a list of rows
    rows = []
    for edge in result['data']['invoices']['edges']:
        node = edge['node']
        rows.append({
            'id': node.get('id'),
            'invnumber': node.get('number'),
	    'externalid': node.get('externalId'),
            'ponumber': node.get('poNumber'),
	    'status': node.get('status'),
	    'paidstatus': node.get('paidStatus'),
	    'alreadypaidamount': node.get('alreadyPaidAmount'),
	    'created': node.get('created'),
            'total': node.get('total')
        })

    # Convert list of dicts into a pandas DataFrame (NEW)
    df = pd.DataFrame(rows)

    print(f"Fetched {len(df)} rows from API.")
    return df


# Optional: test standalone
if __name__ == "__main__":
    from login import get_token
    token, rouid = get_token()
    df = fetch(token, rouid)
    print(df.head())
