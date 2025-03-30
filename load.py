def load_data_to_duckdb(file_list, connection):
    """
    Loads Pokemon data from multiple Parquet files into a DuckDB database.
    Creates required tables if they don't exist and updates metadata.

    Args:
        file_list (list): List of paths to Parquet files containing Pokemon data.
        connection: DuckDB connection object.

    Returns:
        None
    """
    if not file_list:
        raise Exception("No files provided for loading into DuckDB.")

    # Ensure the pokedex table exists
    connection.execute("""
        CREATE TABLE IF NOT EXISTS pokedex (
            last_id INTEGER,
            pokemon_id INTEGER,
            name TEXT,
            url TEXT
        )
    """)

    # Ensure the metadata table exists
    connection.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            last_id INTEGER
        )
    """)

    # Process each file in the file list
    for file_path in file_list:
        normalized_path = file_path.replace("\\", "/")
        # Extract the last_id from the file name (or metadata)
        new_last_id = int(normalized_path.split("_")[-1].split(".")[0])  # Example: Extract ID from file like 'pokedex_0000000010.parquet'

        # Insert data from the Parquet file into the pokedex table
        connection.execute("""
            INSERT INTO pokedex 
            SELECT 
                ? as last_id,
                CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER) as pokemon_id,
                name,
                url
            FROM read_parquet(?)
        """, [new_last_id, normalized_path])

        # Preview loaded data
        preview_df = connection.execute("""
            SELECT * FROM pokedex
            WHERE last_id = ?
            LIMIT 5
        """, [new_last_id]).fetchdf()
        print(f"📊 Preview loaded data for file {normalized_path}:")
        print(preview_df)

        # Update metadata table
        result = connection.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        if result > 0:
            connection.execute("""
                UPDATE metadata SET last_id = ? WHERE last_id IS NOT NULL
            """, [new_last_id])
        else:
            connection.execute("INSERT INTO metadata VALUES (?)", [new_last_id])

    print("✅ All files loaded successfully.")
    """
    Loads Pokemon data from multiple Parquet files into a DuckDB database.
    This function creates a 'pokedex' table if it doesn't exist, loads data from Parquet files,
    and updates the metadata table with the last processed ID for each file.

    Args:
        file_list (list): List of paths to Parquet files containing Pokemon data.
        connection: DuckDB connection object.

    Returns:
        None
    """
    if not file_list:
        raise Exception("No files provided for loading into DuckDB.")

    # Ensure the pokedex table exists
    connection.execute("""
        CREATE TABLE IF NOT EXISTS pokedex (
            last_id INTEGER,
            pokemon_id INTEGER,
            name TEXT,
            url TEXT
        )
    """)

    # Process each file in the file list
    for file_path in file_list:
        normalized_path = file_path.replace("\\", "/")
        # Extract the last_id from the file name (or metadata)
        new_last_id = int(normalized_path.split("_")[-1].split(".")[0])  # Example: Extract ID from file like 'pokedex_0000000010.parquet'

        # Insert data from the Parquet file into the pokedex table
        connection.execute("""
            INSERT INTO pokedex 
            SELECT 
                ? as last_id,
                CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER) as pokemon_id,
                name,
                url
            FROM read_parquet(?)
        """, [new_last_id, normalized_path])

        # Preview loaded data
        preview_df = connection.execute("""
            SELECT * FROM pokedex
            WHERE last_id = ?
            LIMIT 5
        """, [new_last_id]).fetchdf()
        print(f"📊 Preview loaded data for file {normalized_path}:")
        print(preview_df)

        # Update metadata table
        connection.execute("""
            UPDATE metadata SET last_id = ? WHERE last_id IS NOT NULL
        """, [new_last_id])

        result = connection.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        if result == 0:
            connection.execute("INSERT INTO metadata VALUES (?)", [new_last_id])

    print("✅ All files loaded successfully.")