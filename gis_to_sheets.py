import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
import time
from requests.exceptions import RequestException

# Load credentials from environment variables
service_account_info = json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

# Google Sheets setup
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GIS_URL = os.getenv("GIS_REST_URL")

if not service_account_info:
    raise ValueError("Missing GCP_SERVICE_ACCOUNT_JSON environment variable.")

if not SPREADSHEET_ID:
    raise ValueError("Missing SPREADSHEET_ID environment variable.")

if not GIS_URL:
    raise ValueError("Missing GIS_REST_URL environment variable.")


# Parameters to fetch all records
params = {
    "f": "json",
    "where": "1=1",  # Adjust this filter based on your data
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "geometryType": "esriGeometryEnvelope",
    "geometry": None,  # Specify geometry if needed
    "inSR": "102100",
    "outFields": "*",  # Fetch all fields
    "orderByFields": "objectid ASC",
    "outSR": "102100",
    "resultRecordCount": 1000,  # Records per request
}

def safe_request(url, params, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            if response.status_code == 429:  # Rate limit
                print(f"Rate limit hit, retrying in {delay * (2 ** attempt)} seconds...")
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
                continue
            response.raise_for_status()  # Raise an error for bad status codes
            return response
        except RequestException as e:
            print(f"Request failed: {e}. Retrying... ({attempt + 1}/{retries})")
            time.sleep(delay)  # Wait before retrying
    return None


def safe_sheet_append(sheet, row, retries=3, delay=5):
    for attempt in range(retries):
        try:
            sheet.append_row(row)
            time.sleep(1)  # Add delay to avoid quota exhaustion
            return True
        except Exception as e:
            print(f"Failed to append row: {e}. Retrying... ({attempt + 1}/{retries})")
            time.sleep(delay)
    return False


def fetch_gis_data():
    all_data = []
    offset = 0

    while True:
        params["resultOffset"] = offset
        response = safe_request(GIS_URL, params)

        if response:
            data = response.json()
            if "features" in data and len(data["features"]) > 0:
                for feature in data["features"]:
                    attributes = feature["attributes"]
                    all_data.append(attributes)
                offset += len(data["features"])  # Increment offset for the next batch
            else:
                print("No more records to fetch.")
                break
        else:
            print(f"Failed to fetch data after retries. Exiting.")
            break

    update_google_sheet(all_data)

def update_google_sheet(all_data):
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet("Sheet1")
    sheet.clear()  # Clears previous data

    if all_data:
        headers = list(all_data[0].keys())  # Extract headers from the first data record
        sheet.append_row(headers)  # Add column headers
        
        values = [[record.get(header, '') for header in headers] for record in all_data]
        
        # Use batch_update to insert all rows at once
        sheet.append_rows(values)  # Efficient bulk insert
        
        print(f"GIS Data Updated Successfully with {len(all_data)} records!")
    else:
        print("No GIS data found to update.")


if __name__ == "__main__":
    fetch_gis_data()

