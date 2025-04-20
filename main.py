# main.py

import duckdb
# Assuming your modified file is saved as tomtom_extracts.py
# Import the new extraction function and optionally the CONFIG for default areas
from tomtom_extracts import extract_traffic_data_for_areas, CONFIG as EXTRACT_CONFIG

# Keep the imports for load and transform (though their internal logic will change later)
from load import load_data_to_duckdb # Note: This file's logic still needs updating for traffic data structure
from transform import transform_data_in_duckdb # Note: This file's logic still needs updating for traffic data structure


def main():
    print("Starting TomTom Traffic ETL process...")

    # --- Define the areas of interest ---
    # Instead of batch size/count, you now define the geographic areas to process.
    # This list should contain the bounding box strings or other identifiers
    # expected by your construct_api_url function in tomtom_extracts.py.
    # You can use the default defined in tomtom_extracts.py or define others here.
    areas_to_extract = [
        EXTRACT_CONFIG["DEFAULT_AREA_BBOX"], # Using the default from the config
        # Add more areas if needed, e.g.:
        # "lat3,lon3,lat4,lon4",
        # "lat5,lon5,lat6,lon6",
    ]

    # --- Extraction Phase ---
    print("--- Extraction Phase ---")
    # Call the new extraction function with the list of areas
    extracted_files = extract_traffic_data_for_areas(areas_to_extract)

    # You can optionally print the list of files that were generated
    # print("Extracted Parquet files:\n", extracted_files)

    # Check if any files were extracted before proceeding
    if not extracted_files:
        print("No files were extracted. Aborting loading and transformation.")
        return # Exit the main function if no files

    # --- Loading Phase ---
    print("\n--- Loading Phase ---")
    # It's a good idea to change the database name to reflect the data
    conn = duckdb.connect("traffic_data.db")

    # Pass the list of extracted file paths to the loading function
    # *** NOTE: The load_data_to_duckdb function's internal logic
    #     still needs to be updated to handle the new traffic data schema. ***
    load_data_to_duckdb(extracted_files, conn)

    # --- Transformation Phase ---
    print("\n--- Transformation Phase ---")
    # Pass the database connection to the transformation function
    # *** NOTE: The transform_data_in_duckdb function's internal logic
    #     still needs to be updated for traffic data transformations. ***
    transform_data_in_duckdb(conn)

    # --- Cleanup ---
    conn.close()
    print("\nETL process finished.")


if __name__ == "__main__":
    main()