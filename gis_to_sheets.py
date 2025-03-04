import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# Load credentials from environment variables
service_account_info = json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

# Google Sheets setup
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GIS_URL = os.getenv("GIS_REST_URL")

def fetch_gis_data():
    response = requests.get(GIS_URL)
    data = response.json()

    # Connect to Google Sheets
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1
    sheet.clear()  # Clears previous data

    # Extract and write GIS data
    features = data.get('features', [])
    if not features:
        print("No features found")
        return

    headers = list(features[0]['attributes'].keys())
    sheet.append_row(headers)  # Add column headers

    for feature in features:
        row = [feature['attributes'].get(header, '') for header in headers]
        sheet.append_row(row)

    print("GIS Data Updated Successfully!")

if __name__ == "__main__":
    fetch_gis_data()
