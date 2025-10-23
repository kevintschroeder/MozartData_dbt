# login.py
import requests
import json
import os


def get_token():
    """
    Authenticates with the Beanworks API and returns an access token + root org unit ID.
    
    Environment variables required:
        BEANWORKS_USERNAME
        BEANWORKS_PASSWORD
    """

    url_signin = "https://beanworks.ca/signin"

    username = os.environ.get("BEANWORKS_USERNAME")
    password = os.environ.get("BEANWORKS_PASSWORD")

    if not username or not password:
        raise ValueError("Missing BEANWORKS_USERNAME or BEANWORKS_PASSWORD environment variable")

    data = {
        "username": username,
        "password": password
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(url_signin, data=data, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Login failed: {response.status_code}\n{response.text}")

    parsed_response = json.loads(response.text)

    access_token = parsed_response.get("accessToken")
    root_org_unit_id = parsed_response.get("rootOrgUnitId")

    if not access_token or not root_org_unit_id:
        raise Exception("Login response did not contain accessToken or rootOrgUnitId")

    print("✅ Successfully authenticated with Beanworks API")

    return access_token, root_org_unit_id


# Optional: allow testing this script standalone
if __name__ == "__main__":
    token, rouid = get_token()
    print(f"Access token (first 20 chars): {token[:20]}...")
    print(f"Root Org Unit ID: {rouid}")
