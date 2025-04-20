# extracts.py (with explicit unit labels)

import os
import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import traceback
import glob

# --- Configuration ---
load_dotenv()

CONFIG = {
    "TOMTOM_API_KEY": os.getenv("TOMTOM_API_KEY"),
    "TOMTOM_TRAFFIC_API_BASE_URL": "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute",

    # Define example geographic POINTS for a potential route (lat,lon)
    # These are example points along Dien Bien Phu in HCMC
    # Units: Degrees for lat/lon
    "ROUTE_POINTS_EXAMPLE": [
        "10.79187,106.68831", # Near Cach Mang Thang Tam
        "10.78792,106.70215", # Middle stretch
        "10.79096,106.71560"  # Near bridge/boundary
    ],

    "api_timeout_seconds": 10, # Unit added to config name
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
    # point_identifier is a string like "lat,lon" (degrees)
    timestamp = datetime.datetime.now().strftime(CONFIG["file_name_timestamp_format"])
    safe_point_id = point_identifier.replace('.', '_').replace(',', '-')
    file_name = f"traffic_data_{safe_point_id}_{timestamp}.{CONFIG['extension']}"
    file_path = os.path.join(CONFIG["folder"], file_name)
    return file_path


def save_to_parquet(df, file_path):
    """
    Saves a pandas DataFrame to a Parquet file.
    Input DataFrame columns have units as per TomTom API (km/h, seconds, etc.)
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


def construct_api_url(point_lat_lon_str, zoom=10, format='xml', **kwargs):
    """
    Constructs the TomTom Traffic API URL for /flowSegmentData/absolute endpoint.

    Args:
        point_lat_lon_str (str): Point coordinate string (lat,lon in degrees).
        zoom (int): Zoom level (unitless).
        format (str): Data format (string).

    Returns:
        str: Constructed API URL including API key.
    """
    base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    key = CONFIG["TOMTOM_API_KEY"]
    url = f"{base}/{zoom}/{format}"

    query_params = f"key={key}&point={point_lat_lon_str}" # Point is lat,lon string
    for param, value in kwargs.items():
        query_params += f"&{param}={value}"

    url += f"?{query_params}"
    print(f"Constructed URL: {url}")
    return url


def fetch_data_from_api(url):
    """
    Fetches data from the given API URL. Returns raw response text (XML).
    API timeout is in seconds (CONFIG['api_timeout_seconds']).
    """
    print(f"🌐 Fetching data from: {url} (Timeout: {CONFIG['api_timeout_seconds']} seconds)")
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout_seconds"])
        response.raise_for_status()
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        return None


# --- Data Parsing Function ---

def parse_traffic_response_to_dataframe(xml_data):
    """
    Parses the XML response from the TomTom Traffic API into a pandas DataFrame.
    Assigns raw API units to columns:
    - currentSpeed, freeFlowSpeed: km/h
    - currentTravelTime, freeFlowTravelTime: seconds (per segment)
    - confidence, frc, roadClosure, coordinate_count: unitless
    - coordinates: list of (latitude, longitude) tuples (degrees)
    """
    if not xml_data:
        print("No XML data provided for parsing.")
        return pd.DataFrame()

    records = []
    try:
        root = ET.fromstring(xml_data)
        segment_data = {}

        scalar_tags = ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                       'freeFlowTravelTime', 'confidence', 'roadClosure']

        for tag in scalar_tags:
            element = root.find(tag)
            segment_data[tag] = element.text if element is not None else None

        coordinates_list = []
        coords_element = root.find('coordinates')
        if coords_element is not None:
            for coord_elem in coords_element.findall('coordinate'):
                lat_elem = coord_elem.find('latitude')
                lon_elem = coord_elem.find('longitude')
                # Units: degrees
                lat = float(lat_elem.text) if lat_elem is not None and lat_elem.text else None
                lon = float(lon_elem.text) if lon_elem is not None and lon_elem.text else None
                if lat is not None and lon is not None:
                    coordinates_list.append((lat, lon)) # Store as (lat, lon) tuples (degrees)

        segment_data['coordinate_count'] = len(coordinates_list) # Unitless
        segment_data['coordinates'] = coordinates_list # List of (degree, degree) tuples

        records.append(segment_data)

        df = pd.DataFrame(records)
        print(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns after parsing.")
        print(f"DataFrame column units: currentSpeed, freeFlowSpeed (km/h); currentTravelTime, freeFlowTravelTime (seconds per segment).")


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
        traceback.print_exc()
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

    Args:
        points_to_process (list): List of geographic point strings (lat,lon in degrees).

    Returns:
        list: List of file paths (strings) for all successfully extracted and saved data files.
    """
    file_paths = []

    if not points_to_process:
        print("No points specified for extraction.")
        return file_paths

    print(f"--- Starting ETL Extraction for {len(points_to_process)} point(s) ---")

    for point_identifier in points_to_process: # point_identifier is string "lat,lon" (degrees)
        try:
            api_url = construct_api_url(point_lat_lon_str=point_identifier, zoom=10, format='xml')
            print(f"\nProcessing point: {point_identifier} (lat, lon in degrees)")

            xml_data = fetch_data_from_api(api_url)

            if xml_data:
                df = parse_traffic_response_to_dataframe(xml_data)

                if not df.empty:
                    file_path = construct_file_path(point_identifier) # point_identifier is string "lat,lon"
                    save_to_parquet(df, file_path)
                    file_paths.append(file_path)
                else:
                    print(f"No data or failed to parse data for point: {point_identifier}.")
            else:
                print(f"Failed to fetch data for point: {point_identifier}.")

        except Exception as e:
            print(f"❌ An unexpected error occurred while processing point {point_identifier}: {e}")
            traceback.print_exc()
            continue

    print("\n✅ ETL Extraction phase completed.")
    return file_paths

# --- Simple Transformation Function (Example) ---

def transform_traffic_data(file_paths):
    """
    Reads data from saved Parquet files, combines it, and calculates averages.
    Derives estimated travel time based on average speed and a *known* route distance.

    Args:
        file_paths (list): A list of paths (strings) to the Parquet files from a single extraction run
                           (each file containing data for one point).

    Returns:
        pd.DataFrame: A DataFrame containing the combined raw data.
        dict: A dictionary containing calculated average metrics with units in keys.
        float/None: Estimated travel time in seconds, or None if calculation is not possible.
    """
    print("\n--- Starting Transformation ---")

    if not file_paths:
        print("No files provided for transformation.")
        return pd.DataFrame(), {}, None

    all_dataframes = []
    for f_path in file_paths:
        try:
            print(f"📖 Reading file: {f_path}")
            df = pd.read_parquet(f_path)
            all_dataframes.append(df)
        except Exception as e:
            print(f"❌ Error reading file {f_path}: {e}")
            traceback.print_exc()
            continue

    if not all_dataframes:
         print("No dataframes successfully read.")
         return pd.DataFrame(), {}, None

    # Combine all dataframes into one
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Combined data from {len(all_dataframes)} files into DataFrame with shape {combined_df.shape}")

    # --- Calculate Averages ---
    # Column units: currentSpeed, freeFlowSpeed (km/h); currentTravelTime, freeFlowTravelTime (seconds per segment)
    numeric_cols_for_avg = ['currentSpeed', 'freeFlowSpeed', 'currentTravelTime', 'freeFlowTravelTime', 'confidence']
    averages = {}
    for col in numeric_cols_for_avg:
        if col in combined_df.columns:
             # Add unit to the key name for clarity
             if col in ['currentSpeed', 'freeFlowSpeed']:
                 avg_key = f"average_{col}_kmph"
             elif col in ['currentTravelTime', 'freeFlowTravelTime']:
                 avg_key = f"average_{col}_seconds_per_segment" # Clarify this is per the API's segment
             elif col == 'confidence':
                 avg_key = f"average_{col}_unitless"
             else:
                 avg_key = f"average_{col}" # Default if unit unknown/unnecessary

             averages[avg_key] = combined_df[col].mean()
        else:
             averages[f"average_{col}"] = None # Or np.nan

    print("\nCalculated Averages from Sampled Points:")
    for key, value in averages.items():
        print(f"  {key}: {value}")

    # --- Derive Estimated Travel Time for the Full Route ---
    # This REQUIRES the total distance of the route from start (d3) to end (d2).
    # This distance is NOT provided by the flowSegmentData API.
    # You must replace this placeholder with the actual distance (e.g., obtained from a mapping service).
    route_distance_meters = 5000 # <<< REPLACE WITH ACTUAL ROUTE DISTANCE IN METERS (e.g., 5000 for 5km)

    # Get the average current speed across the sampled points
    avg_current_speed_kmph = averages.get("average_currentSpeed_kmph") # Unit: km/h

    estimated_travel_time_seconds = None
    if avg_current_speed_kmph is not None and not pd.isna(avg_current_speed_kmph) and avg_current_speed_kmph >= 0 and route_distance_meters is not None and route_distance_meters > 0:
         # Convert average speed from km/h to meters per second (m/s)
         # Formula: km/h * (1000 m / 1 km) * (1 hour / 3600 seconds) = m/s
         avg_current_speed_mps = avg_current_speed_kmph * 1000 / 3600 # Unit: m/s

         if avg_current_speed_mps > 0: # Avoid division by zero if average speed is 0
            # Time (seconds) = Distance (m) / Speed (m/s)
            estimated_travel_time_seconds = route_distance_meters / avg_current_speed_mps # Unit: seconds
            print(f"\nEstimated travel time for route (assuming distance {route_distance_meters:.2f} meters):")
            print(f"  Average Current Speed Used: {avg_current_speed_kmph:.2f} km/h")
            print(f"  Estimated Time: {estimated_travel_time_seconds:.2f} seconds")
            print(f"  Estimated Time: {estimated_travel_time_seconds/60:.2f} minutes")
         else:
             print("\nCannot estimate travel time: Average current speed is effectively zero.")
    else:
        print("\nCannot estimate travel time: Average current speed is missing/invalid or route distance is missing/zero.")


    print("\n--- Transformation Complete ---")

    return combined_df, averages, estimated_travel_time_seconds


# --- Main Execution Block ---
if __name__ == "__main__":
    print("Running extracts.py directly...")

    # --- Part 1: Extraction ---
    print("\n--- Starting Extraction Process ---")
    points_for_route = CONFIG["ROUTE_POINTS_EXAMPLE"]
    extracted_file_paths = extract_traffic_data_for_areas(points_for_route)

    print(f"\nExtracted data saved to files: {extracted_file_paths}")

    # --- Part 2: Transformation ---
    # Call the transformation function imported from transform.py
    print("\n--- Starting Transformation Process ---")
    if extracted_file_paths:
        # Call the imported transform function
        transformed_data_df, calculated_averages_dict, estimated_time_sec = transform_traffic_data(extracted_file_paths)

        # You can now use these results in extracts.py if needed
        # print("\nTransformation Results received back in extracts.py:")
        # print("Estimated Time (seconds):", estimated_time_sec)
    else:
        print("No files were extracted, skipping transformation.")
