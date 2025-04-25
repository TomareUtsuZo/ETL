# transform.py

import pandas as pd
import os # Might be needed for path manipulation later, but not strictly by transform_traffic_data itself
import glob # Useful if you want to find files based on patterns later
import traceback

def transform_traffic_data(file_paths):
    """
    Reads data from saved Parquet files, combines it, and calculates averages.
    Derives estimated travel time based on average speed and a *known* route distance.

    Args:
        file_paths (list): A list of paths (strings) to the Parquet files from a single extraction run
                           (each file containing data for one point). These should be full paths.

    Returns:
        pd.DataFrame: A DataFrame containing the combined raw data.
        dict: A dictionary containing calculated average metrics with units in keys.
        float/None: Estimated travel time in seconds, or None if calculation is not possible.
    """
    print("\n--- Starting Transformation ---")

    if not file_paths:
        print("No files provided for transformation.")
        # Returning empty dataframe, empty dict, and None estimated time
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
            continue # Skip this file and continue with others

    if not all_dataframes:
         print("No dataframes successfully read.")
         # Returning empty dataframe, empty dict, and None estimated time
         return pd.DataFrame(), {}, None


    # Combine all dataframes into one
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Combined data from {len(all_dataframes)} files into DataFrame with shape {combined_df.shape}")

    # --- Calculate Averages ---
    # Column units from API: currentSpeed, freeFlowSpeed (km/h); currentTravelTime, freeFlowTravelTime (seconds per segment)
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

             averages[avg_key] = combined_df[col].mean() # .mean() ignores NaN values

        # Add None/NaN for columns expected but not found/parsed
        elif f"average_{col}" not in averages: # Prevent overwriting if a unit-specific key was used
             averages[f"average_{col}"] = None # Or pd.NA


    print("\nCalculated Averages from Sampled Points:")
    for key, value in averages.items():
        print(f"  {key}: {value}")

    # --- Derive Estimated Travel Time for the Full Route ---
    # This REQUIRES the total distance of the route from start (d3) to end (d2).
    # This distance is NOT provided by the flowSegmentData API.
    # You must replace this placeholder with the actual distance (e.g., obtained from a mapping service).
    route_distance_meters = 5000 # <<< REPLACE WITH ACTUAL ROUTE DISTANCE IN METERS (e.g., 5000 for 5km)
    # Define the route distance outside the function or pass it as an argument if it varies

    # Get the average current speed across the sampled points
    # Use .get() with a default to avoid KeyError if the key doesn't exist due to parsing issues
    avg_current_speed_kmph = averages.get("average_currentSpeed_kmph") # Unit: km/h

    estimated_travel_time_seconds = None
    # Check if average speed is a valid number and non-negative
    if isinstance(avg_current_speed_kmph, (int, float)) and avg_current_speed_kmph >= 0 and route_distance_meters is not None and route_distance_meters > 0:
         # Convert average speed from km/h to meters per second (m/s)
         # Formula: km/h * (1000 m / 1 km) * (1 hour / 3600 seconds) = m/s
         avg_current_speed_mps = avg_current_speed_kmph * 1000 / 3600 # Unit: m/s

         if avg_current_speed_mps > 0: # Avoid division by zero
            # Time (seconds) = Distance (m) / Speed (m/s)
            estimated_travel_time_seconds = route_distance_meters / avg_current_speed_mps # Unit: seconds
            print(f"\nEstimated travel time for route (assuming distance {route_distance_meters:.2f} meters):")
            print(f"  Average Current Speed Used: {avg_current_speed_kmph:.2f} km/h")
            print(f"  Estimated Time: {estimated_travel_time_seconds:.2f} seconds")
            print(f"  Estimated Time: {estimated_travel_time_seconds/60:.2f} minutes")
         else:
             print("\nCannot estimate travel time: Average current speed is effectively zero (indicates significant delay).")
    else:
        print("\nCannot estimate travel time: Average current speed is missing/invalid, or route distance is missing/zero.")


    print("\n--- Transformation Complete ---")

    # Return the combined data, averages, and estimated time
    return combined_df, averages, estimated_travel_time_seconds

# Note: The __main__ block is typically in a orchestrator script (like main.py or run.py)
# or kept simple in the module itself for testing the module's functions independently.
# For testing this transform module directly, you would typically generate some dummy files
# or point it to a known directory of previously extracted files.
# Example basic __main__ for testing transform.py:
if __name__ == "__main__":
    print("Running transform.py directly for testing...")
    # This requires you to manually specify paths to files you want to transform.
    # For example, if you have files in 'traffic_data' like:
    # traffic_data_10_79_106_68_20231027_100000.parquet
    # traffic_data_10_78_106_70_20231027_100005.parquet
    # traffic_data_10_79_106_71_20231027_100010.parquet
    # You would list them here:
    example_file_paths = glob.glob('traffic_data/traffic_data_10_79_106_68_*.parquet') # Example: find all for one point
    # Or specify exact files from a specific run:
    # example_file_paths = [
    #     'traffic_data/traffic_data_10_79_106_68_20231027_100000.parquet',
    #     'traffic_data/traffic_data_10_78_106_70_20231027_100005.parquet',
    #     'traffic_data/traffic_data_10_79_106_71_20231027_100010.parquet'
    # ]

    if example_file_paths:
         print(f"Attempting to transform files: {example_file_paths}")
         transformed_df, calculated_averages, estimated_time = transform_traffic_data(example_file_paths)
         # You can print results here if needed
         # print("\nTest Transform Results:")
         # print("Combined Data Head:")
         # print(transformed_df.head())
         # print("\nAverages:", calculated_averages)
         # print("Estimated Time (seconds):", estimated_time)
    else:
         print("No example files found to transform. Please run extraction first.")