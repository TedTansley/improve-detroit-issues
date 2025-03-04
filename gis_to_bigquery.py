import os
import json
import requests
import time
from google.oauth2.service_account import Credentials
from google.cloud import bigquery, storage
from requests.exceptions import RequestException
from google.api_core.exceptions import GoogleAPICallError, NotFound, Forbidden
import tempfile


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

schema = [
    bigquery.SchemaField("ID", "INTEGER"),
    bigquery.SchemaField("Status", "STRING"),
    bigquery.SchemaField("Request_Type_Title", "STRING"),
    bigquery.SchemaField("Description", "STRING"),
    bigquery.SchemaField("Web_Url", "STRING"),
    bigquery.SchemaField("Report_Method", "STRING"),
    bigquery.SchemaField("Priority_Code", "INTEGER"),
    bigquery.SchemaField("Created_At", "TIMESTAMP"),
    bigquery.SchemaField("Acknowledged_At", "TIMESTAMP"),
    bigquery.SchemaField("Closed_At", "TIMESTAMP"),
    bigquery.SchemaField("Reopened_At", "TIMESTAMP"),
    bigquery.SchemaField("Updated_At", "TIMESTAMP"),
    bigquery.SchemaField("Days_to_Close", "INTEGER"),
    bigquery.SchemaField("Canonical_Issue_ID", "INTEGER"),
    bigquery.SchemaField("Address", "STRING"),
    bigquery.SchemaField("Neighborhood", "STRING"),
    bigquery.SchemaField("Council_District", "INTEGER"),
    bigquery.SchemaField("Latitude", "FLOAT"),
    bigquery.SchemaField("Longitude", "FLOAT"),
    bigquery.SchemaField("Address_ID", "INTEGER"),
    bigquery.SchemaField("ObjectId", "INTEGER"),
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
        update_bigquery(all_data)
    else:
        print("No GIS data found to update.")

import tempfile
import os

def update_bigquery(all_data):
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

    # Prepare data for insertion
    rows_to_insert = [{key: record[key] for key in record} for record in all_data]

    # Save the rows to a temporary JSON file
    with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="") as temp_file:
        temp_file_path = temp_file.name
        # Save the data as JSON in the temporary file
        json.dump(rows_to_insert, temp_file)

    # Load data from the temporary file into BigQuery
    try:
        # Create a URI pointing to the temporary file
        uri = f"gs://{PROJECT_ID}/temp/{os.path.basename(temp_file_path)}"
        
        # Upload the file to Google Cloud Storage
        storage_client = storage.Client(credentials=creds)
        bucket = storage_client.bucket(f"{PROJECT_ID}")
        blob = bucket.blob(f"temp/{os.path.basename(temp_file_path)}")
        blob.upload_from_filename(temp_file_path)

        # Load data into BigQuery from the Google Cloud Storage URI
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )  # Make sure to pass the job config for proper schema handling

        load_job.result()  # Wait for the load job to complete
        print(f"Data loaded successfully from {uri} to BigQuery!")

    except Exception as e:
        print(f"Error loading data from file to BigQuery: {e}")
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)



if __name__ == "__main__":
    fetch_gis_data()

