# fetch_data.py
import requests, json

def fetch(access_token, root_org_unit_id):
    url_api = 'https://beanworks.ca/api/graphql/'
    url_with_rouid = f'{url_api}?rouid={root_org_unit_id}'

    query = """
    {
        invoices {
            totalCount
            edges {
                node {
                    externalId
                }
            }
        }
    }
    """

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Http-Authorization': f'Bearer {access_token}'
    }

    data = {'query': query}
    response = requests.post(url_with_rouid, headers=headers, data=data)

    if response.status_code != 200:
        raise Exception(f"GraphQL query failed: {response.status_code}\n{response.text}")

    result = response.json()
    invoices = result["data"]["invoices"]["edges"]

    # Return a list of dicts like [{'externalid': '12345'}, ...]
    return [{"externalid": item["node"]["externalId"]} for item in invoices]

print(f"{len(rows)} rows inserted into api_ingest.raw_data")
