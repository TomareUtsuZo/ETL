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
    """
    print("--- Inside parse_traffic_response_to_dataframe ---")

    if not xml_data:
        print("No XML data provided for parsing.")
        return pd.DataFrame()

    print("Attempting to parse XML response...")

    records = []
    try:
        print(f"Type of xml_data: {type(xml_data)}")
        print(f"Start of xml_data: {xml_data[:100]}...")

        root = ET.fromstring(xml_data)
        print(f"XML parsed successfully. Root tag: {root.tag}")

        segment_data = {}

        # --- Extract simple scalar elements ---
        print("Extracting scalar elements...") # Refined print
        scalar_tags = ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                       'freeFlowTravelTime', 'confidence', 'roadClosure']

        for tag in scalar_tags:
            element = root.find(tag)
            # --- ADD THESE DEBUG PRINTS ---
            print(f"  Looking for tag: <{tag}>")
            if element is not None:
                print(f"    Found tag: <{tag}>. Text content: '{element.text}'")
            else:
                print(f"    Tag <{tag}> not found.")
            # ----------------------------
            segment_data[tag] = element.text if element is not None else None

        print("Finished extracting scalar elements.") # Refined print


        # --- Handling Coordinates ---
        print("Processing coordinates...") # Refined print
        coordinates_list = []
        coords_element = root.find('coordinates')
        if coords_element is not None:
             print("  <coordinates> element found.") # ADD THIS PRINT
             for i, coord_elem in enumerate(coords_element.findall('coordinate')):
                 lat_elem = coord_elem.find('latitude')
                 lon_elem = coord_elem.find('longitude')
                 lat = float(lat_elem.text) if lat_elem is not None and lat_elem.text else None
                 lon = float(lon_elem.text) if lon_elem is not None and lon_elem.text else None
                 if lat is not None and lon is not None:
                     coordinates_list.append((lat, lon)) # Store as a tuple list
                     # --- ADD THIS DEBUG PRINT (Optional, can be verbose) ---
                     # if i < 5: # Print only the first few coordinates found
                     #     print(f"    Found coordinate {i}: ({lat}, {lon})")
                     # -------------------------------------------------------
                 else:
                     print(f"    Warning: Found coordinate element {i} but lat/lon text was missing or invalid.") # ADD THIS PRINT

        segment_data['coordinate_count'] = len(coordinates_list)
        segment_data['coordinates'] = coordinates_list
        print(f"Processed {len(coordinates_list)} coordinates.") # Refined print


        records.append(segment_data)


        # Create the pandas DataFrame
        print(f"Creating DataFrame from dictionary with keys: {list(segment_data.keys())}") # ADD THIS PRINT
        df = pd.DataFrame(records)
        print(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.") # Updated print

        # --- Optional: Convert data types ---
        print("Converting data types...") # Refined print
        numeric_cols = ['currentSpeed', 'freeFlowSpeed', 'currentTravelTime', 'freeFlowTravelTime', 'confidence']
        for col in numeric_cols:
            if col in df.columns:
                # Use pd.to_numeric with errors='coerce' to turn unparseable values into NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'roadClosure' in df.columns:
             # Convert 'true'/'false' strings to boolean
             df['roadClosure'] = df['roadClosure'].astype(str).str.lower() == 'true'
        print("Finished converting data types.") # Refined print


        print(f"Parsed {len(df)} records from XML.")
        # print(df.head()) # Optional: Print the first few rows of the parsed DataFrame


        print("--- Exiting parse_traffic_response_to_dataframe ---")
        return df

    except ET.ParseError as e:
        print(f"❌ Error parsing XML response: {e}")
        print("--- Exiting parse_traffic_response_to_dataframe with ParseError ---")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed XML data: {e}")
        # print(f"XML Root element: {root.tag if 'root' in locals() and root is not None else 'N/A'}")
        print("--- Exiting parse_traffic_response_to_dataframe with Exception ---")
        # Keep the traceback for now
        import traceback
        traceback.print_exc()
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
    Performs a simple test extraction for a predefined point,
    parses the response, and prints the resulting DataFrame.
    Used by the __main__ block below.
    """
    print("--- Starting Simple TomTom Extraction Test ---")

    test_point = CONFIG["TEST_POINT"]
    print(f"Using test point: {test_point}")

    # Construct the URL for the test point
    # Use parameters appropriate for the /flowSegmentData/absolute endpoint (point, zoom, xml)
    test_url = construct_api_url(point=test_point, zoom=10, format='xml') # Adjust params as needed

    # Fetch the data (returns XML text)
    xml_data = fetch_data_from_api(test_url)

    # Process the fetched data only if it was successful
    if xml_data is not None:
        print("\n✅ Successfully fetched data. Raw XML response:")
        print(xml_data) # Print the XML text directly
        print("\nAttempting to parse the fetched XML data...") # ADD THIS PRINT

        # --- Call the parsing function ---
        parsed_df = parse_traffic_response_to_dataframe(xml_data) # ADD THIS LINE
        # -----------------------------------

        # Check if parsing was successful and resulted in a non-empty DataFrame
        if not parsed_df.empty:
            print("\n✅ Successfully parsed data. Resulting DataFrame head:") # ADD THIS PRINT
            print(parsed_df.head()) # Print the head of the DataFrame
            print(f"\nDataFrame shape: {parsed_df.shape}") # ADD THIS PRINT
            print("\n--- Simple Test Complete: Data Parsed ---") # ADD THIS PRINT
        else:
             print("\n❌ Parsing resulted in an empty DataFrame.") # ADD THIS PRINT
             print("Please check the parse_traffic_response_to_dataframe function logic.")
             print("\n--- Simple Test Complete: Parsing Failed ---") # ADD THIS PRINT
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