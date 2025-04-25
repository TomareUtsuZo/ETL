# scripts/extract_weather.py

import os
import datetime
import requests
import pandas as pd
import json # Weather APIs commonly return JSON
from dotenv import load_dotenv # Keep for standalone testing, but Airflow handles env vars
import traceback

# --- Configuration ---
# In an Airflow environment, it's best to rely on Airflow Variables or Connection
# details passed via environment variables or op_kwargs, rather than loading
# from .env within the task script or trying to import shared configs.
# For this script to work as an Airflow task, it should get necessary info
# (like API key, base URL, output folder) from environment variables set by Airflow
# or passed as arguments.

# Define a minimal CONFIG structure for clarity and potential standalone testing
# In Airflow, these values should ideally come from Airflow Variables set in the UI
# or passed via op_kwargs from the DAG.
# For simplicity here, we'll rely on environment variables which Airflow can set.
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
# Ensure WEATHER_API_BASE_URL is set as an environment variable in your Airflow setup
WEATHER_API_BASE_URL = os.getenv("WEATHER_API_BASE_URL", "https://api.exampleweather.com/v1/current") # Default if not set
OUTPUT_FOLDER = os.getenv("WEATHER_OUTPUT_FOLDER", "source_data/weather") # Default folder
FILE_EXTENSION = "parquet"
FILE_NAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
API_TIMEOUT_SECONDS = int(os.getenv("WEATHER_API_TIMEOUT_SECONDS", 10)) # Default timeout


# --- Initial Checks ---
# Perform checks within the main task function, not at the top level,
# so they run when the task executes, not when the DAG file is parsed.
# os.makedirs(OUTPUT_FOLDER, exist_ok=True) # Create folder within the task function


# --- Helper Functions (Adapted from your script) ---

def construct_weather_api_url(location_coords: dict, api_key: str, base_url: str, **kwargs) -> str:
    """
    Constructs the Weather API URL for a given location coordinates.
    <<< YOU NEED TO ADAPT THIS FUNCTION FOR YOUR SPECIFIC WEATHER API >>>

    Args:
        location_coords (dict): Dictionary with 'lat' and 'lon' keys (in degrees).
        api_key (str): Your Weather API key.
        base_url (str): The base URL for the weather API endpoint.
        **kwargs: Additional parameters for the API request (e.g., units, language).

    Returns:
        str: Constructed API URL.
    """
    lat = location_coords.get("lat")
    lon = location_coords.get("lon")

    if lat is None or lon is None:
        raise ValueError(f"Invalid location coordinates provided: {location_coords}. Requires 'lat' and 'lon'.")

    # Example for an API using lat/lon parameters (like OpenWeatherMap)
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key, # Example parameter name for API key
        "units": "metric", # Example parameter for units (metric, imperial)
        **kwargs # Include any additional parameters
    }

    # Construct the full URL with parameters
    # requests.Request handles URL encoding of parameters
    req = requests.Request('GET', base_url, params=params)
    prepared_url = req.prepare().url

    print(f"Constructed Weather API URL: {prepared_url}")
    return prepared_url


def fetch_data_from_api(url: str, timeout: int) -> str or None:
    """
    Fetches data from the given API URL. Returns raw response text (JSON).

    Args:
        url (str): API URL to fetch data from.
        timeout (int): Request timeout in seconds.

    Returns:
        str: Raw response text (JSON), or None if request fails.
    """
    print(f"🌐 Fetching data from: {url} (Timeout: {timeout} seconds)")
    try:
        response = requests.get(url=url, timeout=timeout)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        # Weather APIs usually return JSON
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        traceback.print_exc()
        return None


def parse_weather_response_to_dataframe(json_data: str, location_coords: dict) -> pd.DataFrame:
    """
    Parses the JSON response from the Weather API into a pandas DataFrame.
    Adds location coordinates and fetch timestamp.
    <<< YOU NEED TO ADAPT THIS FUNCTION FOR YOUR SPECIFIC WEATHER API RESPONSE STRUCTURE >>>

    Args:
        json_data (str): Raw JSON response text.
        location_coords (dict): The coordinates used for the location (for adding to DataFrame).

    Returns:
        pd.DataFrame: DataFrame containing parsed weather data, or empty DataFrame on failure/no data.
    """
    print("--- Inside parse_weather_response_to_dataframe ---")

    if not json_data:
        print("No JSON data provided for parsing.")
        return pd.DataFrame()

    try:
        data = json.loads(json_data)
        print("JSON loaded successfully.")
        # print("Raw JSON data structure (first 500 chars):", json_data[:500] + ('...' if len(json_data) > 500 else '')) # Optional debug print

        # <<< ADAPT THIS SECTION BASED ON YOUR WEATHER API'S JSON STRUCTURE >>>
        # This is a hypothetical example structure based on common weather APIs (like OpenWeatherMap)
        records = []
        # Assuming the API returns a single weather object for the location
        record = {
            'latitude': location_coords.get('lat'), # Add the latitude from input
            'longitude': location_coords.get('lon'), # Add the longitude from input
            'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), # Record fetch time (UTC)

            # Example fields - adjust based on your API's JSON structure
            # Use .get() with default values to handle potentially missing keys safely
            'temperature_celsius': data.get('main', {}).get('temp'),
            'feels_like_celsius': data.get('main', {}).get('feels_like'),
            'pressure_hpa': data.get('main', {}).get('pressure'), # Example: pressure in hPa
            'humidity_percent': data.get('main', {}).get('humidity'),
            'wind_speed_mps': data.get('wind', {}).get('speed'), # Example: wind.speed (often m/s or mph)
            'wind_deg': data.get('wind', {}).get('deg'), # Example: wind direction in degrees
            'weather_description': data.get('weather', [{}])[0].get('description'), # Example: weather[0].description
            'weather_icon': data.get('weather', [{}])[0].get('icon'), # Example: weather[0].icon
            'cloudiness_percent': data.get('clouds', {}).get('all'), # Example: clouds.all
            'city_name': data.get('name'), # Example: city name from response
            # Add other relevant fields from your API response
        }
        records.append(record)
        # <<< END OF ADAPTATION SECTION >>>


        df = pd.DataFrame(records)

        # --- Optional: Convert data types ---
        # Ensure numeric columns are actually numeric
        numeric_cols = [
            'latitude', 'longitude', 'temperature_celsius', 'feels_like_celsius',
            'pressure_hpa', 'humidity_percent', 'wind_speed_mps', 'wind_deg',
            'cloudiness_percent'
        ]
        for col in numeric_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert timestamp to datetime objects if needed later, or keep as string for staging
        # df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], errors='coerce')


        print(f"Successfully parsed {len(df)} record(s) into DataFrame.")
        # print(df.head()) # Optional: Print head for debugging

        print("--- Exiting parse_weather_response_to_dataframe ---")
        return df

    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON response: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed weather data: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def construct_weather_file_path(location_coords: dict, output_folder: str, extension: str, timestamp_format: str) -> str:
    """
    Constructs the full file path for saving the weather data for a given location.
    Args:
        location_coords: A dictionary with 'lat' and 'lon' keys.
        output_folder: The base directory for saving files.
        extension: The file extension (e.g., 'parquet').
        timestamp_format: The format string for the timestamp in the filename.
    Returns:
        The full file path string.
    """
    # Use a timestamp to make filenames unique per extraction run
    timestamp = datetime.datetime.now().strftime(timestamp_format)
    # Sanitize coordinates for use in a filename
    lat = location_coords.get('lat', 'unknown').replace('.', '_').replace('-', 'minus')
    lon = location_coords.get('lon', 'unknown').replace('.', '_').replace('-', 'minus')

    file_name = f"weather_data_lat{lat}_lon{lon}_{timestamp}.{extension}"
    file_path = os.path.join(output_folder, file_name)
    return file_path


def save_weather_to_parquet(df: pd.DataFrame, file_path: str):
    """
    Saves a pandas DataFrame to a Parquet file.
    Args:
        df: The pandas DataFrame to save.
        file_path: The path where the Parquet file should be saved.
    """
    try:
        print(f"💾 Saving weather data to {file_path}")
        df.to_parquet(file_path, index=False)
        print("✅ Weather data saved successfully.")
    except ImportError:
        print("❌ Error saving data: Parquet engine (like 'pyarrow' or 'fastparquet') not installed.")
        print("Install one using: pip install pyarrow")
        # In a real DAG, you might want to raise an exception here
        raise # Re-raise the exception to fail the task
    except Exception as e:
        print(f"❌ Error saving weather data to Parquet: {e}")
        traceback.print_exc()
        # In a real DAG, you might want to raise an exception here
        raise # Re-raise the exception to fail the task


# --- Main Weather Extraction Function (For Airflow DAG to import) ---

def extract_weather_data(location_coords: dict) -> str or None:
    """
    Extracts weather data for a single given location.
    Fetches data, parses it, and saves it to a Parquet file.

    Args:
        location_coords: A dictionary with 'lat' and 'lon' keys (in degrees).
                         Example: {"lat": 10.79187, "lon": 106.68831}

    Returns:
        The path to the saved Parquet file (string), or None if extraction/saving failed.
        This return value is intended to be pushed to Airflow XCom.
    """
    lat = location_coords.get("lat")
    lon = location_coords.get("lon")
    location_name = location_coords.get("name", f"lat{lat}_lon{lon}") # Use name if available

    if lat is None or lon is None:
        print(f"Invalid location coordinates provided: {location_coords}. Skipping extraction.")
        return None

    print(f"--- Starting Weather Extraction for location: {location_name} ({lat},{lon}) ---")

    if not WEATHER_API_KEY:
        print("❌ WEATHER_API_KEY is not set as an environment variable. Cannot perform weather extraction.")
        # In Airflow, this likely means the Variable wasn't set or passed correctly.
        return None

    # Ensure output folder exists when the task runs
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    print(f"Weather data output folder '{OUTPUT_FOLDER}' ensured.")


    try:
        # Construct the API URL for the specific location
        api_url = construct_weather_api_url(
            location_coords=location_coords,
            api_key=WEATHER_API_KEY,
            base_url=WEATHER_API_BASE_URL
            # Add any other necessary API parameters here
        )

        # Fetch data from the API (returns JSON text)
        json_data = fetch_data_from_api(api_url, API_TIMEOUT_SECONDS)

        if json_data: # Check if fetching was successful and returned text
            # Parse the JSON response into a DataFrame
            df = parse_weather_response_to_dataframe(json_data, location_coords)

            if not df.empty:
                # Construct file path based on location and timestamp
                file_path = construct_weather_file_path(
                    location_coords=location_coords,
                    output_folder=OUTPUT_FOLDER,
                    extension=FILE_EXTENSION,
                    timestamp_format=FILE_NAME_TIMESTAMP_FORMAT
                )
                # Save the DataFrame to a Parquet file
                save_weather_to_parquet(df, file_path)

                print(f"✅ Extraction and saving successful for location: {location_name}. File: {file_path}")
                return file_path # Return the file path for XCom

            else:
                # This happens if fetch was successful but parsing returned an empty DataFrame
                print(f"No data or failed to parse data for location: {location_name}. Check parse_weather_response_to_dataframe.")
                return None # Return None if no data/parsing failed

        else:
             # This happens if fetch_data_from_api returned None due to a request error
             print(f"Failed to fetch data for location: {location_name}. See error message above.")
             return None # Return None if fetching failed


    except Exception as e:
        # Catch any unexpected errors during the processing of a single location
        print(f"❌ An unexpected error occurred during extraction for location {location_name}: {e}")
        traceback.print_exc()
        # In Airflow, raising an exception here will cause the task to fail.
        # If you return None, the task will succeed but indicate no file was generated.
        # Raising is generally preferred for actual errors.
        raise # Re-raise the exception to fail the task


# --- Simple Test Function (For running this file directly) ---
# This block runs ONLY when you execute 'python scripts/extract_weather.py' directly.
if __name__ == "__main__":
    print("Running scripts/extract_weather.py directly for testing...")
    print("Note: This requires WEATHER_API_KEY and WEATHER_API_BASE_URL set as environment variables.")

    # Load .env for standalone testing if not already loaded
    load_dotenv()

    # Define example location for testing
    # Use a dictionary format like the DAG expects
    test_location = {"lat": 10.79187, "lon": 106.68831, "name": "Example City"}

    extracted_file = extract_weather_data(test_location)

    if extracted_file:
        print(f"\nStandalone test extraction completed. File generated: {extracted_file}")
        # Optional: Read the saved file to verify
        try:
            df_test = pd.read_parquet(extracted_file)
            print("\nSaved Data Head:")
            print(df_test.head())
        except Exception as e:
            print(f"Error reading saved test file: {e}")
    else:
        print("\nStandalone test extraction failed.")

    print("\nscripts/extract_weather.py test finished.")
