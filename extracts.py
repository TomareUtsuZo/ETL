import requests
import pandas as pd
import duckdb
import os

# Global Configuration
CONFIG = {
    "batch_size": 100,           # Number of records to fetch per batch
    "folder": "data",            # Folder to save Parquet files
    "extension": "parquet",      # File extension
    "api_timeout": 10,           # Timeout for API requests (seconds)
    "default_start_id": 0,       # Default ID to start fetching from
    "file_name_padding": 4       # Padding length for file names
}

# Ensure the data folder exists
os.makedirs(CONFIG["folder"], exist_ok=True)

def construct_api_url(limit, offset):
    """
    Constructs the API URL for fetching data.

    Args:
        limit (int): Number of records to fetch.
        offset (int): Starting point for fetching records.

    Returns:
        str: Constructed API URL.
    """
    return f"https://pokeapi.co/api/v2/pokemon?limit={limit}&offset={offset}"

def fetch_data_from_api(url):
    """
    Fetches JSON data from the given API URL.

    Args:
        url (str): API URL to fetch data from.

    Returns:
        dict: JSON response from the API.
    """
    print("🌐 Fetching data from:", url)
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout"])
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        raise
    return response.json()

def construct_file_path(last_id, config=CONFIG):
    """
    Constructs the file path for saving data.

    Args:
        last_id (int): ID used to name the file.
        config (dict): Configuration dictionary for file settings.

    Returns:
        str: Constructed file path.
    """
    if last_id is None:
        raise ValueError("last_id must not be None when constructing file paths.")
    return os.path.join(config["folder"], f"pokedex_{str(last_id).zfill(config['file_name_padding'])}.{config['extension']}")

def save_to_parquet(df, file_path):
    """
    Saves a DataFrame to a Parquet file.

    Args:
        df (DataFrame): DataFrame to save.
        file_path (str): Path where the Parquet file will be saved.
    """
    table = duckdb.from_df(df).to_arrow_table()
    duckdb.from_arrow(table).write_parquet(file_path)
    print(f"💾 Saved to: {file_path}")

def update_metadata_and_get_offset():
    """
    Updates the metadata table and retrieves the next offset.

    Returns:
        int: Starting offset for the next batch.
    """
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (last_id INTEGER)")
    last_id = conn.execute(f"SELECT COALESCE(MAX(last_id), {CONFIG['default_start_id']}) FROM metadata").fetchone()[0]
    conn.close()
    return last_id

def extract_data(limit=CONFIG["batch_size"], last_id=None):
    """
    Extracts Pokemon data from the PokeAPI and saves it to a Parquet file.

    Args:
        limit (int): Number of Pokemon records to fetch.
        last_id (int, optional): ID to start fetching from. Defaults to None.

    Returns:
        tuple: Extracted DataFrame and last_id of the next batch.
    """
    if last_id is None:
        last_id = update_metadata_and_get_offset()

    # Construct API URL and fetch data
    url = construct_api_url(limit, last_id)
    data = fetch_data_from_api(url)

    # Transform data to DataFrame
    df = pd.DataFrame(data["results"])

    # Construct file path and save DataFrame
    file_path = construct_file_path(last_id)
    save_to_parquet(df, file_path)

    # Return extracted DataFrame and next ID
    return df, last_id + limit

def query_all_records():
    """
    Iteratively extracts all Pokemon records from the PokeAPI and saves them as Parquet files.

    Returns:
        list: List of Parquet file paths for all extracted records.
    """
    batch_size = CONFIG["batch_size"]
    last_id = None  # Start from the last saved ID
    file_paths = []

    while True:
        # Ensure last_id is valid
        df, next_id = extract_data(limit=batch_size, last_id=last_id)
        if last_id is None:
            last_id = CONFIG["default_start_id"]  # Default to 0 if undefined

        # Get file path and append to list
        file_path = construct_file_path(last_id)
        file_paths.append(file_path)
        last_id = next_id

        # Stop if fewer records than `limit` are returned
        if len(df) < batch_size:
            print("🎉 All records have been fetched.")
            break

    return file_paths