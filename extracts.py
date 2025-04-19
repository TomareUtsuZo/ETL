import os
import requests
import pandas as pd
import duckdb
import datetime
import re # Needed for parsing filename metadata later
from dotenv import load_dotenv # Recommended for securely loading API key

# --- Configuration ---
# Load environment variables from a .env file (install python-dotenv: pip install python-dotenv)
load_dotenv()

# Global Configuration
CONFIG = {
    # --- TomTom Specific Config ---
    "TOMTOM_API_KEY": os.getenv("TOMTOM_API_KEY"), # !!! LOAD SECURELY FROM ENVIRONMENT VARIABLE !!!
    # Example URL - You MUST choose the specific TomTom Traffic API endpoint you need
    # e.g., Traffic Flow API (unique or non-unique), Traffic Incidents API, etc.
    "TOMTOM_TRAFFIC_API_BASE_URL": "https://api.tomtom.com/traffic/services/4/flow/unique", # Consult TomTom Docs
    # Define areas of interest as bounding boxes (lat1,lon1,lat2,lon2)
    # You will likely need to replace this with your desired area(s)
    "DEFAULT_AREA_BBOX": "52.4107,4.8423,52.4107,4.8423", # Example bounding box

    # --- General Config (Adapt as needed) ---
    "api_timeout": 10,          # Timeout for API requests (seconds)
    "folder": "traffic_data",   # Folder to save Parquet files
    "extension": "parquet",     # File extension
    # File naming will use area identifier and timestamp, not padding
    "file_name_timestamp_format": "%Y%m%d_%H%M%S" # Timestamp format for filenames
}

# Ensure API key is loaded
if not CONFIG["TOMTOM_API_KEY"]:
    raise ValueError("TOMTOM_API_KEY environment variable not set.")

# Ensure the data folder exists
os.makedirs(CONFIG["folder"], exist_ok=True)

# --- Helper Functions ---

def construct_api_url(bbox, zoom=10, format='json', include_every_segment=True):
    """
    Constructs the TomTom Traffic API URL for fetching data.
    This function is highly dependent on the specific TomTom endpoint used.

    Args:
        bbox (str): Bounding box string (lat1,lon1,lat2,lon2).
        zoom (int): Zoom level (often required for spatial APIs).
        format (str): Data format (e.g., 'json').
        include_every_segment (bool): Example parameter for Flow API.

    Returns:
        str: Constructed API URL including API key.
    """
    # --- !!! CONSULT TOMTOM API DOCUMENTATION FOR EXACT URL STRUCTURE !!! ---
    # This is an example for the Flow API unique endpoint.
    base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    key = CONFIG["TOMTOM_API_KEY"]
    # Parameters needed will vary by endpoint
    params = f"key={key}&includeEverySegment={str(include_every_segment).lower()}"

    # Example URL format: BASE_URL/version/layer/style/zoom/bbox/format?key=API_KEY&...
    # Or BASE_URL/version/bbox/format?key=API_KEY
    # This needs to match the *exact* endpoint you are using.
    url = f"{base}/{zoom}/{bbox}/{format}?{params}"

    return url

def fetch_data_from_api(url):
    """
    Fetches JSON data from the given API URL.

    Args:
        url (str): API URL to fetch data from.

    Returns:
        dict: JSON response from the API, or None if request fails.
    """
    print("🌐 Fetching data from:", url)
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout"])
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        # Depending on error handling needs, you might re-raise, return None, or log
        return None # Return None on failure

def construct_file_path(area_identifier):
    """
    Constructs the file path for saving traffic data.
    Filename is based on area identifier and timestamp.

    Args:
        area_identifier (str): A string identifier for the geographic area (e.g., part of the bounding box).

    Returns:
        str: Constructed file path.
    """
    timestamp = datetime.datetime.now().strftime(CONFIG["file_name_timestamp_format"])
    # Make area_identifier safe for filenames (replace characters like .,)
    safe_area_id = area_identifier.replace('.', '_').replace(',', '-')
    return os.path.join(CONFIG["folder"], f"traffic_{safe_area_id}_{timestamp}.{CONFIG['extension']}")

def save_to_parquet(df, file_path):
    """
    Saves a DataFrame to a Parquet file using DuckDB.

    Args:
        df (DataFrame): DataFrame to save.
        file_path (str): Path where the Parquet file will be saved.
    """
    if df.empty:
        print(f"⚠️ DataFrame is empty, not saving to {file_path}")
        return

    try:
        # Using DuckDB to save Arrow table to Parquet
        # Convert pandas DataFrame to DuckDB Relation, then to Arrow Table
        table = duckdb.from_df(df).arrow()
        # Use DuckDB to write the Arrow table to Parquet
        duckdb.from_arrow(table).write_parquet(file_path)
        print(f"💾 Saved {len(df)} records to: {file_path}")
    except Exception as e:
        print(f"❌ Failed to save data to Parquet {file_path}: {e}")


# --- Data Parsing Function (Needs Complete Rewrite) ---

def parse_traffic_response_to_dataframe(api_response_data):
    """
    Parses the JSON response from the TomTom Traffic API into a pandas DataFrame.
    --- !!! THIS FUNCTION IS CRITICAL AND DEPENDS ENTIRELY ON THE TOMTOM API RESPONSE STRUCTURE !!! ---

    Args:
        api_response_data (dict): JSON response data from fetch_data_from_api.

    Returns:
        pd.DataFrame: DataFrame containing parsed traffic data. Returns empty DataFrame on failure or if no data.
    """
    if api_response_data is None:
        return pd.DataFrame()

    # --- !!! IMPLEMENT PARSING LOGIC HERE !!! ---
    # The structure varies greatly between TomTom endpoints (Flow, Incidents, etc.)
    # Examples:
    # - Flow API often returns GeoJSON or a structure with a 'flowSegmentData' key
    # - Incidents API returns a list of incidents.

    # Example (Conceptual - Adapt based on actual API response):
    try:
        # --- Example for a GeoJSON-like Flow API response with 'features' ---
        if 'features' in api_response_data:
            records = []
            for feature in api_response_data.get('features', []):
                properties = feature.get('properties', {})
                flow_data = properties.get('flowSegmentData', {}) # Structure within properties might vary
                # Extract fields relevant to you
                record = {
                    'street': flow_data.get('street'),
                    'frc': flow_data.get('frc'), # Functional Road Class
                    'currentSpeed': flow_data.get('currentSpeed'),
                    'freeFlowSpeed': flow_data.get('freeFlowSpeed'),
                    'currentTravelTime': flow_data.get('currentTravelTime'),
                    'freeFlowTravelTime': flow_data.get('freeFlowTravelTime'),
                    'confidence': flow_data.get('confidence'),
                    'jamFactor': flow_data.get('jamFactor'),
                    'trafficRestriction': flow_data.get('trafficRestriction'),
                    # Add other fields from the response you need
                    # You might also need to handle geometry data if necessary (e.g., using shapely)
                }
                records.append(record)
            df = pd.DataFrame(records)

        # --- Example for an API that returns a list of items directly (like Incidents) ---
        # elif 'incidents' in api_response_data: # Hypothetical Incidents API structure
        #     df = pd.json_normalize(api_response_data.get('incidents', []))

        # --- Example for a response containing a single data key ---
        # elif 'flowSegmentData' in api_response_data: # Example for a single segment response
        #     df = pd.json_normalize(api_response_data['flowSegmentData'])

        else:
            print("Warning: Unexpected TomTom API response structure. Could not parse.")
            df = pd.DataFrame() # Return empty if parsing fails

        return df

    except Exception as e:
        print(f"❌ Error parsing TomTom API response: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


# --- Main Extraction Logic (Modified Iteration) ---

# The original update_metadata_and_get_offset and extract_data are removed/replaced
# because the logic of sequential extraction by ID is gone.

def extract_traffic_data_for_areas(areas_to_process):
    """
    Iteratively extracts traffic data for a list of defined geographic areas.
    Replaces the query_all_records logic which was based on sequential IDs.

    Args:
        areas_to_process (list): List of geographic area identifiers (e.g., bounding box strings).

    Returns:
        list: List of Parquet file paths for all successfully extracted data files.
    """
    file_paths = []

    if not areas_to_process:
        print("No areas specified for extraction.")
        return file_paths

    print(f"Starting data extraction for {len(areas_to_process)} area(s)...")

    for area_identifier in areas_to_process:
        try:
            # Construct the URL for the specific area
            # You might pass other parameters like zoom level, style, etc. here
            api_url = construct_api_url(bbox=area_identifier)

            print(f"\n--- Processing area: {area_identifier} ---")

            # Fetch data from the API
            api_data = fetch_data_from_api(api_url)

            if api_data:
                # Parse the API response into a DataFrame
                df = parse_traffic_response_to_dataframe(api_data)

                if not df.empty:
                    # Construct file path based on area and timestamp
                    file_path = construct_file_path(area_identifier)
                    # Save the DataFrame to a Parquet file
                    save_to_parquet(df, file_path)
                    file_paths.append(file_path)
                else:
                    print(f"No data or failed to parse data for area: {area_identifier}")
            else:
                 print(f"Failed to fetch data for area: {area_identifier}")


        except Exception as e:
            print(f"❌ An error occurred while processing area {area_identifier}: {e}")
            # Decide how to handle errors - continue to next area or stop?
            # For now, we continue to the next area after printing the error.
            continue

    print("\n✅ Extraction phase completed.")
    return file_paths