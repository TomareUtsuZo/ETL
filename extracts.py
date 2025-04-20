# extracts.py (Revised with reduced prints and traceback)

import os
import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import traceback # Ensure traceback is imported

# --- Configuration ---
load_dotenv()

CONFIG = {
    "TOMTOM_API_KEY": os.getenv("TOMTOM_API_KEY"),
    "TOMTOM_TRAFFIC_API_BASE_URL": "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute",
    "TEST_POINT": "52.41072,4.84239",
    "api_timeout": 10,
    "folder": "traffic_data",
    "extension": "parquet",
    "file_name_timestamp_format": "%Y%m%d_%H%M%S"
}

# --- Initial Checks ---
if not CONFIG["TOMTOM_API_KEY"]:
    raise ValueError("TOMTOM_API_KEY environment variable not set.")

os.makedirs(CONFIG["folder"], exist_ok=True)

# --- Helper Functions (Implemented) ---

def construct_file_path(point_identifier):
    """
    Constructs the full file path for saving the data for a given point.
    """
    timestamp = datetime.datetime.now().strftime(CONFIG["file_name_timestamp_format"])
    safe_point_id = point_identifier.replace('.', '_').replace(',', '-')
    file_name = f"traffic_data_{safe_point_id}_{timestamp}.{CONFIG['extension']}"
    file_path = os.path.join(CONFIG["folder"], file_name)
    return file_path


def save_to_parquet(df, file_path):
    """
    Saves a pandas DataFrame to a Parquet file.
    """
    try:
        print(f"💾 Saving data to {file_path}")
        df.to_parquet(file_path, index=False)
        print("✅ Data saved successfully.")
    except ImportError:
        print("❌ Error saving data: Parquet engine (like 'pyarrow' or 'fastparquet') not installed.")
        print("Install one using: pip install pyarrow")
    except Exception as e:
        print(f"❌ Error saving data to Parquet: {e}")
        # traceback.print_exc() # Optional: print traceback for saving errors


def construct_api_url(point, zoom=10, format='xml', **kwargs):
    """
    Constructs the TomTom Traffic API URL for /flowSegmentData/absolute endpoint.
    """
    base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    key = CONFIG["TOMTOM_API_KEY"]
    url = f"{base}/{zoom}/{format}"

    query_params = f"key={key}&point={point}"
    for param, value in kwargs.items():
        query_params += f"&{param}={value}"

    url += f"?{query_params}"
    print(f"Constructed URL: {url}")
    return url


def fetch_data_from_api(url):
    """
    Fetches data from the given API URL. Returns raw response text (XML).
    """
    print("🌐 Fetching data from:", url)
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout"])
        # Optional prints - remove or keep based on desired verbosity
        # print(f"Response Status Code: {response.status_code}")
        # print(f"Response Headers: {response.headers}")

        response.raise_for_status()

        return response.text

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        return None


# --- Data Parsing Function ---

def parse_traffic_response_to_dataframe(xml_data):
    """
    Parses the XML response from the TomTom Traffic API into a pandas DataFrame.
    """
    if not xml_data:
        print("No XML data provided for parsing.")
        return pd.DataFrame()

    # print("Attempting to parse XML response...") # Reduced print

    records = []
    try:
        root = ET.fromstring(xml_data)
        # print(f"XML parsed successfully. Root tag: {root.tag}") # Reduced print

        segment_data = {}

        # --- Extract simple scalar elements ---
        scalar_tags = ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                       'freeFlowTravelTime', 'confidence', 'roadClosure']

        for tag in scalar_tags:
            element = root.find(tag)
            segment_data[tag] = element.text if element is not None else None
            # Optional verbose prints - remove or keep as needed
            # if element is not None:
            #      print(f"    Found tag: <{tag}>. Text content: '{element.text}'")

        # --- Handling Coordinates ---
        coordinates_list = []
        coords_element = root.find('coordinates')
        if coords_element is not None:
            for coord_elem in coords_element.findall('coordinate'):
                lat_elem = coord_elem.find('latitude')
                lon_elem = coord_elem.find('longitude')
                lat = float(lat_elem.text) if lat_elem is not None and lat_elem.text else None
                lon = float(lon_elem.text) if lon_elem is not None and lon_elem.text else None
                if lat is not None and lon is not None:
                    coordinates_list.append((lat, lon))

        segment_data['coordinate_count'] = len(coordinates_list)
        segment_data['coordinates'] = coordinates_list
        # print(f"Processed {len(coordinates_list)} coordinates.") # Reduced print

        records.append(segment_data)

        df = pd.DataFrame(records)
        print(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns after parsing.")

        # --- Convert data types ---
        numeric_cols = ['currentSpeed', 'freeFlowSpeed', 'currentTravelTime', 'freeFlowTravelTime', 'confidence']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'roadClosure' in df.columns:
            df['roadClosure'] = df['roadClosure'].astype(str).str.lower() == 'true'

        print(f"Successfully parsed data for {len(df)} record(s).")
        return df

    except ET.ParseError as e:
        print(f"❌ Error parsing XML response: {e}")
        traceback.print_exc() # Add traceback for parsing errors
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed XML data: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# --- Main ETL Extraction Function ---

def extract_traffic_data_for_areas(points_to_process):
    """
    Iteratively extracts traffic data for a list of defined geographic points.
    Saves successfully extracted data to Parquet files.
    """
    file_paths = []

    if not points_to_process:
        print("No points specified for extraction.")
        return file_paths

    print(f"--- Starting ETL Extraction for {len(points_to_process)} point(s) ---")

    for point_identifier in points_to_process:
        try:
            api_url = construct_api_url(point=point_identifier, zoom=10, format='xml')
            print(f"\nProcessing point: {point_identifier}")

            xml_data = fetch_data_from_api(api_url)

            if xml_data:
                df = parse_traffic_response_to_dataframe(xml_data)

                if not df.empty:
                    file_path = construct_file_path(point_identifier)
                    save_to_parquet(df, file_path)
                    file_paths.append(file_path)
                else:
                    print(f"No data or failed to parse data for point: {point_identifier}.")
            else:
                print(f"Failed to fetch data for point: {point_identifier}.")

        except Exception as e:
            print(f"❌ An unexpected error occurred while processing point {point_identifier}: {e}")
            traceback.print_exc() # <<< Added traceback here
            continue # Continue to the next point

    print("\n✅ ETL Extraction phase completed.")
    return file_paths


# --- Simple Test Function ---

def test_tomtom_extraction():
    """
    Performs a simple test extraction for a predefined point.
    Fetches, parses, and prints DataFrame head (does not save).
    """
    print("--- Starting Simple TomTom Extraction Test ---")

    test_point = CONFIG["TEST_POINT"]
    print(f"Using test point: {test_point}")

    test_url = construct_api_url(point=test_point, zoom=10, format='xml')
    xml_data = fetch_data_from_api(test_url)

    if xml_data is not None:
        # Keep the message indicating successful fetch, but don't print the raw data
        print("\n✅ Successfully fetched data. Raw XML response (data display skipped).")
        # --- REMOVED print(xml_data) ---

        print("\nAttempting to parse the fetched XML data...")
        parsed_df = parse_traffic_response_to_dataframe(xml_data)

        if not parsed_df.empty:
            print("\n✅ Successfully parsed data. Resulting DataFrame head:")
            print(parsed_df.head())
            print(f"\nDataFrame shape: {parsed_df.shape}")
            print("\n--- Simple Test Complete: Data Parsed ---")
        else:
            print("\n❌ Parsing resulted in an empty DataFrame.")
            print("Please check the parse_traffic_response_to_dataframe function logic.")
            print("\n--- Simple Test Complete: Parsing Failed ---")
    else:
        print("\n❌ Simple Test Failed: Could not fetch data.")
        print("Please review the error message above and troubleshooting steps.")


if __name__ == "__main__":
    print("Running extracts.py directly for testing...")
    # You can call the test function for a quick check
    # test_tomtom_extraction()

    # Or call the main extraction function with the test point (will now save)
    print("\n--- Running extract_traffic_data_for_areas with test point ---")
    points = [CONFIG["TEST_POINT"]]
    extracted_files = extract_traffic_data_for_areas(points)
    print(f"\nExtracted files: {extracted_files}")