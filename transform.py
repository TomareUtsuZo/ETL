def create_pokemon_stats_table(connection):
    """
    Creates or replaces the 'pokemon_stats' table with aggregated metrics.
    
    Args:
        connection: DuckDB connection object.
    """
    connection.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS
        SELECT 
            COUNT(*) AS total_pokemon, 
            MIN(CAST(REGEXP_EXTRACT(url, '/pokemon/(\\d+)/', 1) AS INTEGER)) AS first_id, 
            MAX(CAST(REGEXP_EXTRACT(url, '/pokemon/(\\d+)/', 1) AS INTEGER)) AS last_id
        FROM pokedex
    """)
    print("Data transformed and stored in 'pokemon_stats' table.")

def preview_pokemon_stats_data(connection):
    """
    Fetches and previews the data from the 'pokemon_stats' table.
    
    Args:
        connection: DuckDB connection object.
    """
    df = connection.execute("SELECT * FROM pokemon_stats").fetchdf()
    print("📈 Preview transformed data:")
    print(df)

def transform_data_in_duckdb(connection):
    """
    Orchestrates the transformation logic by delegating to specific functions
    for table creation and data preview.
    
    Args:
        connection: DuckDB connection object.
    
    Returns:
        str: Name of the created table ('pokemon_stats').
    """
    create_pokemon_stats_table(connection)
    preview_pokemon_stats_data(connection)
    return "pokemon_stats"