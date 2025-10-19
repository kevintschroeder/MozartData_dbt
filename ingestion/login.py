import requests
import json

def get_token():
    """
    Logs in to Beanworks and returns the access token and root org unit ID.
    """
    # GraphQL endpoint URL
    url_signin = 'https://beanworks.ca/signin'

    # Login credentials
    data = {
        'username': 'kevin.schroedre@candelarenewables.com',
        'password': 'Ry@n20191122',
    }

    # Request headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Send POST request
    response = requests.post(url_signin, data=data, headers=headers)

    # Parse JSON response
    parsed_response = json.loads(response.text)

    # Extract token and root org unit ID
    access_token = parsed_response.get("accessToken", "")
    root_org_unit_id = parsed_response.get("rootOrgUnitId", "")

    # Optional: print for verification
    print(f"Access Token: {access_token}")
    print(f"Root Org Unit ID: {root_org_unit_id}")

    # Return values so they can be used by other scripts
    return access_token, root_org_unit_id


# Optional: run standalone for testing
if __name__ == "__main__":
    token, rouid = get_token()
