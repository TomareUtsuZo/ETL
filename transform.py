def transform_data_in_duckdb(connection):
    """
    Transform data from the consolidated table and create aggregated statistics.
    This function creates a new table 'pokemon_stats' with aggregated metrics:
    1. Calculates total count of Pokemon, minimum Pokemon ID, and maximum Pokemon ID.
    2. Prints a preview of the transformed data.

    Args:
        connection: DuckDB connection object.

    Returns:
        str: Name of the created table ('pokemon_stats').
    """
    # Create or replace the 'pokemon_stats' table with transformed data
    connection.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS
        SELECT 
            COUNT(*) AS total_pokemon, 
            MIN(CAST(REGEXP_EXTRACT(url, '/pokemon/(\\d+)/', 1) AS INTEGER)) AS first_id, 
            MAX(CAST(REGEXP_EXTRACT(url, '/pokemon/(\\d+)/', 1) AS INTEGER)) AS last_id
        FROM pokedex
    """)
    print("Data transformed and stored in 'pokemon_stats' table.")

    # Preview the transformed data
    df = connection.execute("SELECT * FROM pokemon_stats").fetchdf()
    print("📈 Preview transformed data:")
    print(df)

    # Return the name of the created table
    return "pokemon_stats"