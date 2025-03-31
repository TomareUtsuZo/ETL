def create_pokedex_table(connection):
    """
    Ensures the 'pokedex' table exists in the database.
    
    Args:
        connection: DuckDB connection object.
    """
    connection.execute("""
        CREATE TABLE IF NOT EXISTS pokedex (
            last_id INTEGER,
            pokemon_id INTEGER,
            name TEXT,
            url TEXT
        )
    """)

def create_metadata_table(connection):
    """
    Ensures the 'metadata' table exists in the database.
    
    Args:
        connection: DuckDB connection object.
    """
    connection.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            last_id INTEGER
        )
    """)

def extract_last_id_from_filename(file_path):
    """
    Extracts the last_id from a given file path.
    
    Args:
        file_path (str): Path to the Parquet file.
    
    Returns:
        int: Extracted last_id.
    """
    normalized_path = file_path.replace("\\", "/")
    return int(normalized_path.split("_")[-1].split(".")[0])

def load_parquet_to_pokedex_table(connection, file_path, last_id):
    """
    Inserts data from a Parquet file into the 'pokedex' table.
    
    Args:
        connection: DuckDB connection object.
        file_path (str): Path to the Parquet file.
        last_id (int): Last ID extracted from the file name.
    """
    connection.execute("""
        INSERT INTO pokedex 
        SELECT 
            ? AS last_id,
            CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER) AS pokemon_id,
            name,
            url
        FROM read_parquet(?)
    """, [last_id, file_path])

def preview_loaded_data(connection, last_id):
    """
    Previews data that was just loaded into the 'pokedex' table.
    
    Args:
        connection: DuckDB connection object.
        last_id (int): Last ID to filter data for preview.
    """
    preview_df = connection.execute("""
        SELECT * FROM pokedex
        WHERE last_id = ?
        LIMIT 5
    """, [last_id]).fetchdf()
    print(f"📊 Preview loaded data for last_id {last_id}:")
    print(preview_df)

def update_metadata_table(connection, last_id):
    """
    Updates the metadata table with the latest last_id.
    
    Args:
        connection: DuckDB connection object.
        last_id (int): Last ID to update in the metadata table.
    """
    result = connection.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
    if result > 0:
        connection.execute("""
            UPDATE metadata SET last_id = ? WHERE last_id IS NOT NULL
        """, [last_id])
    else:
        connection.execute("INSERT INTO metadata VALUES (?)", [last_id])

def load_data_to_duckdb(file_list, connection):
    """
    Orchestrates the loading of multiple Parquet files into the DuckDB database.
    
    Args:
        file_list (list): List of paths to Parquet files containing Pokemon data.
        connection: DuckDB connection object.
    """
    if not file_list:
        raise Exception("No files provided for loading into DuckDB.")
    
    create_pokedex_table(connection)
    create_metadata_table(connection)

    for file_path in file_list:
        last_id = extract_last_id_from_filename(file_path)
        load_parquet_to_pokedex_table(connection, file_path.replace("\\", "/"), last_id)
        preview_loaded_data(connection, last_id)
        update_metadata_table(connection, last_id)
    
    print("✅ All files loaded successfully.")