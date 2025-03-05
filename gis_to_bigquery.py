import os
import json
import requests
import time
import pandas as pd
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from requests.exceptions import RequestException
from google.api_core.exceptions import GoogleAPICallError, NotFound, Forbidden

# Load credentials from environment variables
service_account_info = json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON"))
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/cloud-platform",  # Broadest scope for all GCP services
            "https://www.googleapis.com/auth/bigquery"]  # For BigQuery
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

schema = [
    bigquery.SchemaField("ID", "INTEGER"),
    bigquery.SchemaField("Status", "STRING"),
    bigquery.SchemaField("Request_Type_Title", "STRING"),
    bigquery.SchemaField("Report_Method", "STRING"),
    bigquery.SchemaField("Created_At", "TIMESTAMP"),
    bigquery.SchemaField("Acknowledged_At", "TIMESTAMP"),
    bigquery.SchemaField("Closed_At", "TIMESTAMP"),
    bigquery.SchemaField("Reopened_At", "TIMESTAMP"),
    bigquery.SchemaField("Updated_At", "TIMESTAMP"),
    bigquery.SchemaField("Days_to_Close", "STRING"),
    bigquery.SchemaField("Address", "STRING"),
    bigquery.SchemaField("Neighborhood", "STRING"),
    bigquery.SchemaField("Council_District", "INTEGER"),
    bigquery.SchemaField("Latitude", "NUMERIC"),
    bigquery.SchemaField("Longitude", "NUMERIC"),
    bigquery.SchemaField("Zip_Code", "STRING")
]


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
        save_to_csv(all_data)
    else:
        print("No GIS data found to update.")


def handle_invalid_data(df):
    # Convert milliseconds to seconds for timestamp fields (if they are in milliseconds)
    timestamp_columns = ['Created_At', 'Acknowledged_At', 'Closed_At', 'Reopened_At', 'Updated_At']
    
    for column in timestamp_columns:
        if column in df.columns:
            # Convert the timestamp from milliseconds to seconds
            df[column] = pd.to_datetime(df[column], errors='coerce', unit='ms')  # Coerce invalid values to NaT (Not a Time)

    return df

def save_to_csv(all_data):
    # Convert data to DataFrame
    df = pd.DataFrame(all_data)

    # Handle invalid timestamps and convert to valid ones
    df = handle_invalid_data(df)
    df = df.drop(columns=["Description", "Web_Url","Canonical_Issue_ID","Address_ID","ObjectId"])

    # Save DataFrame to CSV
    local_csv_path = "/tmp/gis_data.csv"  # Temporary path for CSV file
    df.to_csv(local_csv_path, index=False)

    print(f"Data saved as CSV at {local_csv_path}")
    update_bigquery_from_csv(local_csv_path)


def update_bigquery_from_csv(csv_file_path):
    # Load the CSV file into BigQuery
    client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    table_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

    try:
        table = client.get_table(table_ref)  # Get table metadata
        print(f"Table {TABLE_ID} already exists.")
    except NotFound:
        print(f"Table {TABLE_ID} not found. Creating it...")
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Create the table
        print(f"Table {TABLE_ID} created successfully.")

    # Load data from CSV to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Automatically detect schema
    )

    with open(csv_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

    print(f"Data loaded successfully from {csv_file_path} to BigQuery!")

    # Clean up the temporary CSV file
    os.remove(csv_file_path)


if __name__ == "__main__":
    fetch_gis_data()
