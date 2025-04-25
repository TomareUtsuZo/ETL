# dags/snowflake_api_test_dag.py

from __future__ import annotations

import pendulum
import os
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable # To access Airflow Variables for configuration
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # For Snowflake interactions

# Import functions from your scripts
# Ensure your 'scripts' directory is accessible in the Airflow container
from scripts.extract_traffic import extract_traffic_data # This extracts for one location
from scripts.extract_weather import extract_weather_data # This extracts for one location
from scripts.transform_staging import transform_and_combine_for_staging

# Define your Snowflake connection ID configured in Airflow UI
SNOWFLAKE_CONN_ID = 'snowflake_default' # Ensure this matches your Airflow Connection ID

# Define your Snowflake target details (can also be Airflow Variables)
# Make sure these Variables are set in your Airflow UI
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var="YOUR_DATABASE")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var="YOUR_SCHEMA")
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE", default_var="YOUR_WAREHOUSE")

# Define staging and final table names using the Variables
STG_TRAFFIC_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STG_TRAFFIC_DATA'
STG_WEATHER_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STG_WEATHER_DATA'
FINAL_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TRAFFIC_WEATHER'

# Define the location to use for this test
# Use a single location dictionary like expected by your extract scripts
TEST_LOCATION = {"lat": 34.5408, "lon": -112.4685, "name": "Prescott Valley, AZ"}
# Ensure your .env or Airflow Variables have API keys for this extraction to work

# Define folders for raw and transformed data within the Airflow container
# Ensure these folders are mounted as volumes in your docker-compose.yml
RAW_DATA_FOLDER_TRAFFIC = "/opt/airflow/source_data/traffic"
RAW_DATA_FOLDER_WEATHER = "/opt/airflow/source_data/weather"
TRANSFORMED_DATA_FOLDER = "/opt/airflow/transformed_data"


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Define the DAG using the decorator
@dag(
    dag_id='snowflake_api_load_test', # Changed DAG ID
    default_args=default_args,
    description='Tests API extraction, transformation, and Snowflake loading/union for a single point',
    schedule=None, # Run manually for testing
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['snowflake', 'api', 'test', 'elt'],
)
def snowflake_api_load_test_dag():

    @task
    def create_snowflake_staging_tables_task():
        """Creates staging tables in Snowflake if they don't exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # SQL to create staging traffic table - ensure columns match your expected schema
        create_traffic_staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {STG_TRAFFIC_TABLE} (
            SOURCE VARCHAR,
            TIMESTAMP TIMESTAMP_LTZ,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            CURRENT_SPEED FLOAT,
            FREE_FLOW_SPEED FLOAT,
            CURRENT_DELAY FLOAT,
            FREE_FLOW_DELAY FLOAT,
            ROAD_CLOSURE BOOLEAN,
            LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        # SQL to create staging weather table - ensure columns match your expected schema
        create_weather_staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {STG_WEATHER_TABLE} (
            SOURCE VARCHAR,
            TIMESTAMP TIMESTAMP_LTZ,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            TEMPERATURE FLOAT,
            FEELS_LIKE FLOAT,
            PRESSURE FLOAT,
            HUMIDITY FLOAT,
            WIND_SPEED FLOAT,
            WIND_DEG FLOAT,
            WEATHER_DESCRIPTION VARCHAR,
            CITY_NAME VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        print(f"Creating staging tables {STG_TRAFFIC_TABLE} and {STG_WEATHER_TABLE} if they don't exist...")
        hook.run(create_traffic_staging_sql)
        hook.run(create_weather_staging_sql)
        print("Staging tables checked/created successfully.")

    @task
    def create_snowflake_final_table_task():
        """Creates the final combined table in Snowflake if it doesn't exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # SQL to create the final combined table - ensure columns match your expected schema
        create_final_sql = f"""
        CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
            SOURCE VARCHAR, -- 'traffic' or 'weather'
            TIMESTAMP TIMESTAMP_LTZ,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            -- Common fields or fields from traffic
            CURRENT_SPEED FLOAT,
            FREE_FLOW_SPEED FLOAT,
            CURRENT_DELAY FLOAT,
            FREE_FLOW_DELAY FLOAT,
            ROAD_CLOSURE BOOLEAN,
            -- Fields from weather
            TEMPERATURE FLOAT,
            FEELS_LIKE FLOAT,
            PRESSURE FLOAT,
            HUMIDITY FLOAT,
            WIND_SPEED FLOAT,
            WIND_DEG FLOAT,
            WEATHER_DESCRIPTION VARCHAR,
            CITY_NAME VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP_LTZ -- Timestamp when data was loaded to staging
            -- Add other relevant fields, potentially with NULLable types if not present in both sources
        );
        """
        print(f"Creating final combined table {FINAL_TABLE} if it doesn't exist...")
        hook.run(create_final_sql)
        print("Final combined table checked/created successfully.")

    @task
    def extract_traffic_data_task(location: dict) -> str | None:
        """Extracts traffic data for a single location and returns the file path."""
        # Ensure the output folder exists within the task context
        os.makedirs(RAW_DATA_FOLDER_TRAFFIC, exist_ok=True)
        # Call the extraction function from your script
        # Pass the output folder explicitly if your script needs it, or ensure it uses the env var
        # Assuming extract_traffic_data uses the OUTPUT_FOLDER env var or default
        return extract_traffic_data(location_coords=location)

    @task
    def extract_weather_data_task(location: dict) -> str | None:
        """Extracts weather data for a single location and returns the file path."""
         # Ensure the output folder exists within the task context
        os.makedirs(RAW_DATA_FOLDER_WEATHER, exist_ok=True)
        # Call the extraction function from your script
        # Pass the output folder explicitly if your script needs it, or ensure it uses the env var
        # Assuming extract_weather_data uses the OUTPUT_FOLDER env var or default
        return extract_weather_data(location_coords=location)


    @task
    def transform_and_stage_data_task(traffic_file_path: str | None, weather_file_path: str | None):
        """
        Transforms raw data files (using collected paths) and saves them as staged Parquet files.
        Args:
            traffic_file_path: Path to the raw traffic file (from XCom).
            weather_file_path: Path to the raw weather file (from XCom).
        Returns:
            A tuple containing the paths to the staged traffic and weather Parquet files,
            or (None, None) if no data was processed. This is pushed to XCom.
        """
        # transform_and_combine_for_staging expects lists of file paths
        traffic_files = [traffic_file_path] if traffic_file_path else []
        weather_files = [weather_file_path] if weather_file_path else []

        if not traffic_files and not weather_files:
            print("No raw files provided for transformation. Skipping transformation.")
            return (None, None)

        # Ensure the output folder exists within the task context
        os.makedirs(TRANSFORMED_DATA_FOLDER, exist_ok=True)

        print(f"Transforming {len(traffic_files)} traffic file(s) and {len(weather_files)} weather file(s).")

        # Call the transformation function from your script
        staged_traffic_path, staged_weather_path = transform_and_combine_for_staging(
            traffic_file_paths=traffic_files,
            weather_file_paths=weather_files,
            output_folder=TRANSFORMED_DATA_FOLDER # Ensure this directory is mounted in Docker
        )

        print(f"Staged traffic file: {staged_traffic_path}")
        print(f"Staged weather file: {staged_weather_path}")

        # Return the paths to the staged files for downstream loading tasks
        return (staged_traffic_path, staged_weather_path)


    @task
    def load_staged_data_to_snowflake_task(staged_file_info: tuple):
        """
        Loads staged data from local Parquet files into Snowflake staging tables.
        Args:
            staged_file_info: A tuple containing (staged_traffic_path, staged_weather_path),
                              pulled from the transform_and_stage_data_task's XCom.
        """
        staged_traffic_path, staged_weather_path = staged_file_info

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Load staged traffic data if path exists
        if staged_traffic_path and os.path.exists(staged_traffic_path):
            print(f"Loading staged traffic data from {staged_traffic_path} to {STG_TRAFFIC_TABLE}")
            # Snowflake requires a stage to load local files.
            # Using a temporary internal stage for this example
            stage_name = f"@{SNOWFLAKE_SCHEMA}.%API_TEST_STAGING_STAGE" # Use a distinct stage name

            # Create a temporary stage if it doesn't exist
            hook.run(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name};")

            # Put the local file onto the Snowflake stage
            # AUTO_COMPRESS=TRUE is default for PUT
            put_command = f"PUT file://{staged_traffic_path} {stage_name} AUTO_COMPRESS=TRUE;"
            print(f"Executing PUT command: {put_command}")
            hook.run(put_command)
            print("Traffic file uploaded to stage.")

            # Get the filename on the stage (it will be the same as the local filename + .gz)
            staged_filename = os.path.basename(staged_traffic_path)
            staged_filename_compressed = f"{staged_filename}.gz"

            # Copy data from the staged file into the target table
            copy_command = f"""
            COPY INTO {STG_TRAFFIC_TABLE}
            FROM {stage_name}/{staged_filename_compressed}
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = 'CONTINUE'; -- Or 'ABORT_STATEMENT'
            """
            print(f"Executing COPY INTO command for traffic: {copy_command}")
            hook.run(copy_command)
            print(f"Traffic data loaded into {STG_TRAFFIC_TABLE}.")

            # Optional: Remove the file from the stage after loading
            # remove_command = f"REMOVE {stage_name}/{staged_filename_compressed};"
            # print(f"Executing REMOVE command: {remove_command}")
            # hook.run(remove_command)
            # print("Traffic file removed from stage.")
        else:
            print("No staged traffic file path provided or file not found. Skipping traffic load.")


        # Load staged weather data if path exists
        if staged_weather_path and os.path.exists(staged_weather_path):
            print(f"Loading staged weather data from {staged_weather_path} to {STG_WEATHER_TABLE}")
            # Using the same temporary internal stage
            stage_name = f"@{SNOWFLAKE_SCHEMA}.%API_TEST_STAGING_STAGE"

            # Create a temporary stage if it doesn't exist
            hook.run(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name};")

            # Put the local file onto the Snowflake stage
            put_command = f"PUT file://{staged_weather_path} {stage_name} AUTO_COMPRESS=TRUE;"
            print(f"Executing PUT command: {put_command}")
            hook.run(put_command)
            print("Weather file uploaded to stage.")

            # Get the filename on the stage (it will be the same as the local filename + .gz)
            staged_filename = os.path.basename(staged_weather_path)
            staged_filename_compressed = f"{staged_filename}.gz"

            # Copy data from the staged file into the target table
            copy_command = f"""
            COPY INTO {STG_WEATHER_TABLE}
            FROM {stage_name}/{staged_filename_compressed}
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = 'CONTINUE'; -- Or 'ABORT_STATEMENT'
            """
            print(f"Executing COPY INTO command for weather: {copy_command}")
            hook.run(copy_command)
            print(f"Weather data loaded into {STG_WEATHER_TABLE}.")

            # Optional: Remove the file from the stage after loading
            # remove_command = f"REMOVE {stage_name}/{staged_filename_compressed};"
            # print(f"Executing REMOVE command: {remove_command}")
            # hook.run(remove_command)
            # print("Weather file removed from stage.")

        else:
             print("No staged weather file path provided or file not found. Skipping weather load.")


    @task
    def transform_and_union_in_snowflake_task():
        """
        Performs the final transformation and union in Snowflake using SQL.
        Inserts data from staging tables into the final table.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # SQL to insert data from staging tables into the final table
        # This SQL performs the transformation and union logic.
        # It selects relevant columns from each staging table and unions them.
        # Use COALESCE or CASE statements if you need to handle fields that exist in one source but not the other.
        insert_final_sql = f"""
        INSERT INTO {FINAL_TABLE} (
            SOURCE, TIMESTAMP, LATITUDE, LONGITUDE,
            CURRENT_SPEED, FREE_FLOW_SPEED, CURRENT_DELAY, FREE_FLOW_DELAY, ROAD_CLOSURE,
            TEMPERATURE, FEELS_LIKE, PRESSURE, HUMIDITY, WIND_SPEED, WIND_DEG, WEATHER_DESCRIPTION, CITY_NAME,
            LOAD_TIMESTAMP
        )
        SELECT
            SOURCE, TIMESTAMP, LATITUDE, LONGITUDE,
            CURRENT_SPEED, FREE_FLOW_SPEED, CURRENT_DELAY, FREE_FLOW_DELAY, ROAD_CLOSURE,
            NULL AS TEMPERATURE, NULL AS FEELS_LIKE, NULL AS PRESSURE, NULL AS HUMIDITY, NULL AS WIND_SPEED, NULL AS WIND_DEG, NULL AS WEATHER_DESCRIPTION, NULL AS CITY_NAME,
            LOAD_TIMESTAMP
        FROM {STG_TRAFFIC_TABLE}

        UNION ALL

        SELECT
            SOURCE, TIMESTAMP, LATITUDE, LONGITUDE,
            NULL AS CURRENT_SPEED, NULL AS FREE_FLOW_SPEED, NULL AS CURRENT_DELAY, NULL AS FREE_FLOW_DELAY, NULL AS ROAD_CLOSURE,
            TEMPERATURE, FEELS_LIKE, PRESSURE, HUMIDITY, WIND_SPEED, WIND_DEG, WEATHER_DESCRIPTION, CITY_NAME,
            LOAD_TIMESTAMP
        FROM {STG_WEATHER_TABLE};
        """
        print(f"Performing final transformation and union into {FINAL_TABLE} in Snowflake...")
        hook.run(insert_final_sql)
        print("Final transformation and union completed successfully.")


    @task
    def truncate_snowflake_staging_tables_task():
        """Truncates staging tables after data is moved to the final table."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        print(f"Truncating staging tables {STG_TRAFFIC_TABLE} and {STG_WEATHER_TABLE}...")
        hook.run(f"TRUNCATE TABLE {STG_TRAFFIC_TABLE};")
        hook.run(f"TRUNCATE TABLE {STG_WEATHER_TABLE};")
        print("Staging tables truncated successfully.")

    # --- Define Task Dependencies ---

    # Create tables first
    create_stg_tables = create_snowflake_staging_tables_task()
    create_final_tbl = create_snowflake_final_table_task()

    # Extract data after tables are ready
    # Pass the single test location to each extraction task
    extracted_traffic_file = extract_traffic_data_task(location=TEST_LOCATION)
    extracted_weather_file = extract_weather_data_task(location=TEST_LOCATION)

    create_stg_tables >> [extracted_traffic_file, extracted_weather_file] # Extraction depends on staging tables
    create_final_tbl # Final table creation can happen in parallel

    # Transform and stage data after both extractions are complete
    # The transform task automatically pulls the file paths from XCom
    staged_files = transform_and_stage_data_task(
        traffic_file_path=extracted_traffic_file,
        weather_file_path=extracted_weather_file
    )

    [extracted_traffic_file, extracted_weather_file] >> staged_files

    # Load staged data into Snowflake after transformation
    # The load task automatically pulls the tuple from staged_files's XCom
    load_to_snowflake = load_staged_data_to_snowflake_task(staged_file_info=staged_files)

    staged_files >> load_to_snowflake

    # Final transformation/union depends on the loading task completing
    final_transform = transform_and_union_in_snowflake_task()

    load_to_snowflake >> final_transform

    # Truncate staging tables after final transformation/union
    truncate_stg_tables = truncate_snowflake_staging_tables_task()

    final_transform >> truncate_stg_tables


# Instantiate the DAG
snowflake_api_load_test_dag()
