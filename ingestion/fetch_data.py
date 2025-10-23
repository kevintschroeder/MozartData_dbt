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

    # Updated GraphQL query with correct field names
    query = """
    {
        invoices(where: {}) {
            edges {
                node {
                    id
                    externalId
                    amount        # Corrected field
                    created       # Corrected field
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

    # Make sure 'data' exists in response
    if 'data' not in result or 'invoices' not in result['data']:
        print("🔍 Full API response:")
        print(json.dumps(result, indent=2))
        raise KeyError("❌ 'data' key missing in response — check your API query or token.")

    # Extract the data into a list of rows
    rows = []
    for edge in result['data']['invoices']['edges']:
        node = edge['node']
        rows.append({
            'id': node.get('id'),
            'external_id': node.get('externalId'),
            'amount': node.get('amount'),
            'created': node.get('created')
        })

    print(f"Fetched {len(rows)} rows from API.")
    return rows

# Optional: test standalone
if __name__ == "__main__":
    from login import get_token
    token, rouid = get_token()
    fetch(token, rouid)
