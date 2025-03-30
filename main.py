import requests
import pandas as pd
import os
import duckdb
import pyarrow as pa

def main():
    print("Hello world!")
    all_records, file_paths = iterate_extract_data()

    # Create a DuckDB connection
    conn = duckdb.connect()

    # Load data into DuckDB
    conn = duckdb.connect("pokemon_data.db")  # This creates a persistent database file named 'pokemon_data.db'
    load_data_to_duckdb(file_paths, conn)
    

    # Perform data transformations within DuckDB
    transform_data_in_duckdb(conn)
    

    # Close the DuckDB connection
    conn.close()




def get_total_records():
    api_url = "https://pokeapi.co/api/v2/pokemon?limit=1"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data.get("count", 0)
    else:
        raise Exception(f"Failed to fetch total records: {response.status_code}")


def iterate_extract_data():
    all_records = []  # Store all Pokémon records
    file_paths = []  # Store paths to Parquet files
    updated_offset = 0  # Initial offset
    batch_limit = 50  # Limit the number of iterations for testing

    for i in range(batch_limit):
        print(f"Fetching batch {i + 1}...")
        file_path, updated_offset = extract_data(offset=updated_offset)

        if updated_offset == -1:
            print("No more data to fetch.")
            break

        df = pd.read_parquet(file_path)
        all_records.extend(df.to_dict(orient="records"))
        file_paths.append(file_path)  # Track the file path

    return all_records, file_paths

def extract_data(offset=0, batch_size=100, output_dir="data"):
    """
    Extract Pokémon data from the PokeAPI incrementally and save as Parquet files.

    Parameters:
        offset (int): The starting point for fetching Pokémon data.
        batch_size (int): The number of Pokémon to fetch per batch.
        output_dir (str): The directory to save the Parquet files.

    Returns:
        str: The file path of the saved Parquet file.
        int: The updated offset for the next batch.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Define API endpoint with offset and batch size
    api_url = f"https://pokeapi.co/api/v2/pokemon?offset={offset}&limit={batch_size}"

    # Fetch data from the API
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    data = response.json()
    results = data.get("results", [])

    if not results:
        print("No more data to fetch.")
        return None, -1  # Or handle it as per your logic

    # Convert the data into a DataFrame
    df = pd.DataFrame(results)

    # Define the output Parquet file path
    file_path = os.path.join(output_dir, f"pokemon_offset_{offset}.parquet")

    # Save the DataFrame to a Parquet file
    df.to_parquet(file_path, index=False)

    # Return the file path and updated offset
    updated_offset = offset + batch_size
    return file_path, updated_offset

import os

def load_data_to_duckdb(file_list, connection):
    """
    Load multiple Parquet files into DuckDB and create a consolidated table.
    :param file_list: List of file paths to load.
    :param connection: DuckDB connection object.
    """
    for file in file_list:
        # Normalize file path for DuckDB
        normalized_path = file.replace("\\", "/")

        # Load Parquet data into DuckDB table
        table_name = f"data_from_{os.path.basename(normalized_path).replace('.', '_')}"
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{normalized_path}')"
        connection.execute(query)
        
        print(f"Loaded data from {normalized_path} into table {table_name}")

    # Consolidate all tables into a single table
    consolidated_query = "CREATE TABLE consolidated_table AS SELECT * FROM "
    consolidated_query += " UNION ALL ".join(
        [f"(SELECT * FROM {table_name})" for file in file_list]
    )
    connection.execute(consolidated_query)
    print("Consolidated data saved into 'consolidated_table'")

def transform_data_in_duckdb(connection):
    """
    Perform data transformations directly within DuckDB.
    :param connection: DuckDB connection object.
    """
    connection.execute("""
        CREATE TABLE pokemon_stats AS 
        SELECT COUNT(*) AS total_pokemon, 
               MIN(CAST(REGEXP_EXTRACT(url, '[0-9]+$') AS INTEGER)) AS first_pokemon_id, 
               MAX(CAST(REGEXP_EXTRACT(url, '[0-9]+$') AS INTEGER)) AS last_pokemon_id 
        FROM consolidated_table
        WHERE regexp_matches(url, '[0-9]+$')
    """)
    print("Data transformed and stored in 'pokemon_stats' table")


if __name__ == "__main__":
    main()
