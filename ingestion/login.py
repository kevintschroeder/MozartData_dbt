import requests
import json
import os
import pandas as pd

# GraphQL endpoint URL
url_signin = 'https://beanworks.ca/signin'

# Create a dictionary for the form data (username and password)
data = {
    'username': 'kevin.schroedre@candelarenewables.com',
    'password': 'Ry@n20191122',
}

# Set the headers for the request
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

# Send the POST request with the form data
response = requests.post(url_signin, data=data, headers=headers)

response_1 = response.text
 
# Parse JSON response

parsed_response = json.loads(response_1)
 
# Extract values

access_token = parsed_response.get("accessToken", "") #.rstrip(",")  # Remove trailing comma if present

root_org_unit_id = parsed_response.get("rootOrgUnitId", "")
 
# Print values

print(f"Access Token: {access_token}")

print(f"Root Org Unit ID: {root_org_unit_id}")