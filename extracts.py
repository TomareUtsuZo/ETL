# extracts.py (Continued modifications)

import os
import requests
import pandas as pd
import duckdb
import datetime
import re
import json
import xml.etree.ElementTree as ET # Import for XML parsing
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

CONFIG = {
    # --- TomTom Specific Config ---
    "TOMTOM_API_KEY": os.getenv("TOMTOM_API_KEY"), # Loaded securely from .env

    # Update to the WORKING endpoint base URL
    "TOMTOM_TRAFFIC_API_BASE_URL": "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute", # <<< Updated

    # Define a specific geographic POINT for testing (lat,lon)
    # Replace with a point in an area you expect data for.
    "TEST_POINT": "52.41072,4.84239", # Example working point

    # --- General Config (Adapt as needed) ---
    "api_timeout": 10,          # Timeout for API requests (seconds)
    "folder": "traffic_data",   # Folder to save Parquet files
    "extension": "parquet",     # File extension
    "file_name_timestamp_format": "%Y%m%d_%H%M%S" # Timestamp format for filenames
}

# --- Initial Checks ---
if not CONFIG["TOMTOM_API_KEY"]:
    raise ValueError("TOMTOM_API_KEY environment variable not set.")

os.makedirs(CONFIG["folder"], exist_ok=True)

# --- Helper Functions ---

# Modified construct_api_url for the /flowSegmentData/absolute endpoint (point parameter, xml format)
def construct_api_url(point, zoom=10, format='xml', **kwargs):
    """
    Constructs the TomTom Traffic API URL for /flowSegmentData/absolute endpoint.

    Args:
        point (str): Point coordinate string (lat,lon).
        zoom (int): Zoom level.
        format (str): Data format (e.g., 'xml').
        **kwargs: Additional parameters.

    Returns:
        str: Constructed API URL including API key.
    """
    base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    key = CONFIG["TOMTOM_API_KEY"]

    # URL structure: BASE_URL/version/style/zoom/format?key=API_KEY&point=...
    # Example: .../4/absolute/10/xml?key=...&point=...
    # Note: 'absolute' might be part of the path before version/style/zoom/format depending on docs
    # Let's assume it's part of the base for simplicity based on your curl URL
    # The format looks more like BASE_URL/style/zoom/format?key=...&point=... based on your curl
    # Let's adjust the base URL in CONFIG slightly if needed, or adjust this path construction.
    # Assuming BASE_URL = "https://api.tomtom.com/traffic/services/4/"
    # Then URL = BASE_URL + "flowSegmentData/absolute/" + zoom + "/" + format + "?key=..."
    # Let's stick with the BASE_URL from CONFIG and add the rest:
    url = f"{base}/{zoom}/{format}" # Example path parameters

    query_params = f"key={key}&point={point}" # Required key and point
    for param, value in kwargs.items():
        query_params += f"&{param}={value}"

    url += f"?{query_params}"

    # Check the constructed URL against the working curl URL
    print(f"Constructed URL: {url}")
    return url


# Modified fetch_data_from_api to return response text (XML) on success
def fetch_data_from_api(url):
    """
    Fetches data from the given API URL.

    Args:
        url (str): API URL to fetch data from.

    Returns:
        str: Raw response text (XML), or None if request fails.
    """
    print("🌐 Fetching data from:", url)
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout"])

        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")

        # --- REMOVE TEMPORARY DEBUGGING PRINT ---
        # Remove or comment out the block that printed response.text manually
        # because we are now expecting a 200 response and will handle the body
        # in the parsing function.
        # ----------------------------------------

        response.raise_for_status() # Raise an HTTPError for bad responses

        # If successful (200), return the raw response text (which is XML)
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        return None # Return None on failure


# Keep construct_file_path as is
def construct_file_path(area_identifier):
     # ... your function ...
     pass

# Keep save_to_parquet as is
def save_to_parquet(df, file_path):
    # ... your function ...
    pass

# --- Data Parsing Function (Implement XML Parsing) ---

def parse_traffic_response_to_dataframe(xml_data):
    """
    Parses the XML response from the TomTom Traffic API into a pandas DataFrame.
    --- !!! IMPLEMENT XML PARSING LOGIC HERE !!! ---

    Args:
        xml_data (str): Raw XML response text from fetch_data_from_api.

    Returns:
        pd.DataFrame: DataFrame containing parsed traffic data. Returns empty DataFrame on failure or if no data.
    """
    if not xml_data:
        return pd.DataFrame()

    print("Attempting to parse XML response...")

    records = []
    try:
        # Parse the XML string
        root = ET.fromstring(xml_data)

        # --- !!! IMPLEMENT YOUR XML TRAVERSAL AND DATA EXTRACTION LOGIC HERE !!! ---
        # Based on your curl output, the structure is like:
        # <flowSegmentData>
        #   <frc>...</frc>
        #   <currentSpeed>...</currentSpeed>
        #   ...
        #   <coordinates>
        #     <coordinate>...</coordinate>
        #     <coordinate>...</coordinate>
        #     ...
        #   </coordinates>
        # </flowSegmentData>

        # For the 'flowSegmentData' endpoint, you get one primary object per request point.
        # You can extract its attributes directly.
        flow_segment_data = {}
        # Example: Extracting simple elements
        for child in root:
             if child.tag in ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                              'freeFlowTravelTime', 'confidence', 'roadClosure']:
                 flow_segment_data[child.tag] = child.text

        # Handling coordinates - this can be tricky to put into a single row DataFrame
        # For now, maybe skip or process separately, or store as a list/string
        coordinates = []
        coords_element = root.find('coordinates')
        if coords_element is not None:
             for coord_elem in coords_element.findall('coordinate'):
                 lat = coord_elem.find('latitude').text if coord_elem.find('latitude') is not None else None
                 lon = coord_elem.find('longitude').text if coord_elem.find('longitude') is not None else None
                 if lat is not None and lon is not None:
                     coordinates.append({'latitude': float(lat), 'longitude': float(lon)})

        # You might want to add the coordinates as a list or JSON string to the row
        # For a simple DataFrame, you might just add the count or a representation
        flow_segment_data['coordinate_count'] = len(coordinates)
        # flow_segment_data['coordinates_list'] = coordinates # Might be complex column type

        # Since this endpoint gives one segment per point, your DataFrame might have just one row per point
        records.append(flow_segment_data)

        df = pd.DataFrame(records)
        print(f"Parsed {len(df)} records from XML.")

        return df

    except ET.ParseError as e:
        print(f"❌ Error parsing XML response: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed XML data: {e}")
        return pd.DataFrame()


# --- Main ETL Extraction Function (For main.py to import) ---
# This function needs to be adapted to take a LIST OF POINTS or AREAS

# We need to decide how extract_traffic_data_for_areas will work now.
# Will it take a list of BBOXes and somehow get points within them?
# Or will it take a list of specific POINTS?
# The /flowSegmentData/absolute endpoint works based on a single POINT.
# So, the main extraction function should probably take a list of POINTS.

def extract_traffic_data_for_areas(points_to_process):
    """
    Iteratively extracts traffic data for a list of defined geographic points.
    Adapted for the /flowSegmentData/absolute endpoint.

    Args:
        points_to_process (list): List of geographic point strings (lat,lon).

    Returns:
        list: List of Parquet file paths for all successfully extracted data files.
    """
    file_paths = []

    if not points_to_process:
        print("No points specified for extraction.")
        return file_paths

    print(f"--- Starting ETL Extraction for {len(points_to_process)} point(s) ---")

    for point_identifier in points_to_process: # Iterate through points, not areas
        try:
            # Construct the API URL for the specific point.
            # Use parameters appropriate for the /flowSegmentData/absolute endpoint (point, zoom, xml)
            api_url = construct_api_url(point=point_identifier, zoom=10, format='xml') # Adjust zoom/params as needed

            print(f"\nProcessing point: {point_identifier}")

            # Fetch data from the API (returns XML text)
            xml_data = fetch_data_from_api(api_url)

            if xml_data: # Check if fetching was successful and returned text
                # Parse the XML response into a DataFrame
                df = parse_traffic_response_to_dataframe(xml_data)

                if not df.empty:
                    # Construct file path based on point and timestamp
                    # Reuse construct_file_path, maybe clarify 'area_identifier' is a point now
                    file_path = construct_file_path(point_identifier)
                    # Save the DataFrame to a Parquet file
                    save_to_parquet(df, file_path)
                    file_paths.append(file_path)
                else:
                    # This happens if fetch was successful but parsing returned an empty DataFrame
                    print(f"No data or failed to parse data for point: {point_identifier}. Check parse_traffic_response_to_dataframe.")
            else:
                 # This happens if fetch_data_from_api returned None due to a request error
                 print(f"Failed to fetch data for point: {point_identifier}. See error message above.")

        except Exception as e:
            # Catch any unexpected errors during the processing of a single point
            print(f"❌ An unexpected error occurred while processing point {point_identifier}: {e}")
            # Decide how to handle errors - continue to next point or stop?
            continue # Continue to the next point

    print("\n✅ ETL Extraction phase completed.")
    return file_paths


# --- Simple Test Function (For running this file directly) ---
# This function is NOT called by main.py. It's only for independent testing.

def test_tomtom_extraction():
    """
    Performs a simple test extraction for a predefined point and prints the response.
    Used by the __main__ block below.
    """
    print("--- Starting Simple TomTom Extraction Test ---")

    test_point = CONFIG["TEST_POINT"]
    print(f"Using test point: {test_point}")

    # Construct the URL for the test point using the main construct function
    # Use parameters appropriate for the /flowSegmentData/absolute endpoint (point, zoom, xml)
    test_url = construct_api_url(point=test_point, zoom=10, format='xml') # Adjust params as needed

    # Fetch the data (returns XML text)
    xml_data = fetch_data_from_api(test_url)

    # Print the raw response data if successfully fetched (and not caught by fetch_data's error handling)
    if xml_data is not None:
        print("\n✅ Successfully fetched data. Raw XML response:")
        print(xml_data) # Print the XML text directly
        print("\n--- Simple Test Complete: Data Fetched ---")
    else:
        print("\n❌ Simple Test Failed: Could not fetch data.")
        print("Please review the error message above and troubleshooting steps.")


# --- This block runs ONLY when you execute 'python extracts.py' directly ---
if __name__ == "__main__":
    print("Running extracts.py directly for testing...")
    test_tomtom_extraction() # Call the test function to verify basic connection

# --- When main.py imports 'extracts', only the functions defined above
#     (like extract_traffic_data_for_areas, CONFIG, etc.) are available.
#     The code inside if __name__ == "__main__": does NOT run.