import os
import json
import requests
import time
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from requests.exceptions import RequestException

# Load credentials from environment variables
service_account_info = json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/bigquery"]
)

# BigQuery setup
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
GIS_URL = os.getenv("GIS_REST_URL")

if not service_account_info:
    raise ValueError("Missing GCP_SERVICE_ACCOUNT_JSON environment variable.")

if not PROJECT_ID:
    raise ValueError("Missing PROJECT_ID environment variable.")

if not DATASET_ID:
    raise ValueError("Missing DATASET_ID environment variable.")

if not TABLE_ID:
    raise ValueError("Missing TABLE_ID environment variable.")

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

def create_dynamic_schema(features):
    """Create schema dynamically based on the GIS data."""
    if not features:
        return []

    # Grab the first feature's attributes to build the schema
    first_record = features[0]["attributes"]
    schema = []

    for key, value in first_record.items():
        if isinstance(value, int):
            schema.append(bigquery.SchemaField(key, "INTEGER"))
        elif isinstance(value, float):
            schema.append(bigquery.SchemaField(key, "FLOAT"))
        elif isinstance(value, bool):
            schema.append(bigquery.SchemaField(key, "BOOLEAN"))
        elif isinstance(value, str):
            schema.append(bigquery.SchemaField(key, "STRING"))
        elif isinstance(value, dict):
            schema.append(bigquery.SchemaField(key, "STRING"))  # Or use 'JSON' if preferred
        elif isinstance(value, list):
            schema.append(bigquery.SchemaField(key, "STRING"))  # Treat lists as strings or JSON
        elif isinstance(value, type(None)):
            schema.append(bigquery.SchemaField(key, "STRING"))  # Handle nulls as string
        else:
            schema.append(bigquery.SchemaField(key, "STRING"))  # Default to string

    return schema

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

    if all_data:
        update_bigquery(all_data)
    else:
        print("No GIS data found to update.")

def update_bigquery(all_data):
    client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    # Generate schema dynamically based on the GIS data
    schema = create_dynamic_schema(all_data)
    
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    try:
        table = client.get_table(table_ref)  # Get table metadata
        print(f"Table {TABLE_ID} already exists.")
    except Exception as e:
        # If the table doesn't exist, we can create it using dynamic schema
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Create the table
        print(f"Table {TABLE_ID} created successfully.")

    # Prepare data for insertion
    rows_to_insert = []
    for record in all_data:
        row = {key: record.get(key, None) for key in record.keys()}
        rows_to_insert.append(row)

    # Insert data into BigQuery table
    errors = client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        print(f"Data inserted into BigQuery table {DATASET_ID}.{TABLE_ID} successfully!")
    else:
        print(f"Errors occurred while inserting data into BigQuery: {errors}")

if __name__ == "__main__":
    fetch_gis_data()

