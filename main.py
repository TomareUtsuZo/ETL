# main.py

import os
from dotenv import load_dotenv

# Import the extraction function and the CONFIG dictionary from extracts.py
# CONFIG is needed here for shared settings like API key, points, folder
from extracts import extract_traffic_data_for_areas, CONFIG

# Import the transformation function from transform.py
from transform import transform_traffic_data # Note: This replaces the old transform_data_in_duckdb


def main():
    """
    Main function to orchestrate the TomTom traffic data ETL pipeline.
    """
    print("--- Starting TomTom Traffic Data ETL Pipeline ---")

    # --- Configuration and Setup ---
    load_dotenv() # Load environment variables from .env file
    print("Configuration loaded from .env")

    # Perform initial checks using the imported CONFIG
    if not CONFIG.get("TOMTOM_API_KEY"): # Use .get for safer access
        raise ValueError("TOMTOM_API_KEY environment variable not set. Please check your .env file.")
        # In a production app, you might handle this more gracefully

    output_folder = CONFIG.get("folder", "traffic_data") # Use .get with a default
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder '{output_folder}' ensured.")

    # Get the points to process from CONFIG
    points_to_process = CONFIG.get("ROUTE_POINTS_EXAMPLE")
    if not points_to_process:
        print("No points defined in CONFIG['ROUTE_POINTS_EXAMPLE']. Please add points to extracts.py CONFIG.")
        print("Aborting pipeline.")
        return # Exit if no points are defined

    print(f"Pipeline configured to process {len(points_to_process)} point(s).")

    # --- Extraction Phase ---
    print("\n--- Starting Extraction Phase ---")
    # Call the extract function and get the list of saved file paths
    extracted_file_paths = extract_traffic_data_for_areas(points_to_process)

    # Check if the extraction phase was successful and produced files
    if not extracted_file_paths:
        print("\n❌ Extraction Phase failed or produced no files. Aborting transformation.")
        # Consider logging this failure more formally
        return # Exit if extraction failed

    print(f"\n✅ Extraction Phase Complete. {len(extracted_file_paths)} file(s) extracted and saved.")
    # Optional: Print the list of extracted files here in main.py if desired
    # print("Extracted files:", extracted_file_paths)


    # --- Transformation Phase ---
    print("\n--- Starting Transformation Phase ---")
    # Pass the list of extracted file paths to the transformation function
    transformed_data_df, calculated_averages_dict, estimated_time_sec = transform_traffic_data(extracted_file_paths)

    # You can now use the results from the transformation phase in main.py
    if transformed_data_df.empty:
         print("\n❌ Transformation Phase resulted in no data. Check transformation logic and logs.")
    else:
         print("\n✅ Transformation Phase Complete.")
         # Optionally print final results obtained from the transform step
         print("\n--- Final Estimated Travel Time from Transformation ---")
         if estimated_time_sec is not None:
             print(f"Estimated Travel Time for Route: {estimated_time_sec:.2f} seconds ({estimated_time_sec/60:.2f} minutes)")
         else:
             print("Could not estimate travel time from sampled points.")


    print("\n--- ETL Pipeline Finished ---")
    # In a real application, you might add a Load step here
    # (e.g., saving the combined_df or calculated_averages to a database or data warehouse)


if __name__ == "__main__":
    main()