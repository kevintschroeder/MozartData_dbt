# Submit a request to Beanworks GraphQL

# GraphQL endpoint URL
url_api = 'https://beanworks.ca/api/graphql/'

# Replace <RootOU> with the actual Root Organization Unit ID
url_with_rouid = f'{url_api}?rouid={root_org_unit_id}'

print(url_with_rouid)

# Your GraphQL query (as a string)
query = """
{
    invoices (where: {
            _and: [{
                    status: {
                        _in: [Exported]
                    }
                },
                {
                    created:{
                        _gt: "2024-06-09T00:00:00+00:00"
                    }
                }
            ]
        }
  ) {
    totalCount
    edges {
      node {
            id
            externalId
            poNumber
	    status          
            created
            number
            owner{
                id
                fullName
            }          
            vendor {
                structure{
                    id
                    name
                }
            }
            total
        }
    }
 }
}
"""

# Set the headers for the request
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Http-Authorization': f'Bearer {access_token}'  # Bearer token for authorization
}

# Prepare the data to send in the body of the POST request
data = {
    'query': query
}

# Send the POST request to the GraphQL API
response = requests.post(url_with_rouid, headers=headers, data=data)

# Check if the request was successful
if response.status_code == 200:
    # Parse the response as JSON and print the result
    result = response.json()
    print(json.dumps(result, indent=2))  # Pretty print the result
else:
    print(f"GraphQL query failed with status code {response.status_code}")
    print(response.text)