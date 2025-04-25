# dags/traffic_weather_elt_dag.py

from __future__ import annotations

import pendulum
import os # Needed for os.path.join etc.

from airflow.decorators import dag, task
from airflow.models import Variable # To access Airflow Variables for credentials
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # For Snowflake interactions

# Import functions from your scripts
# Ensure your 'scripts' directory is accessible in the Airflow container
from scripts.extract_traffic import extract_traffic_data # This extracts for one location
from scripts.extract_weather import extract_weather_data # This extracts for one location
from scripts.transform_staging import transform_and_combine_for_staging
# We will use SnowflakeHook/Operator directly in tasks for loading/transforming
# from scripts.load_snowflake import ... # No longer need functions that manage connections/cursors directly


# Define the locations you want to process
# You can get this from a config file or pass as DAG run config
LOCATIONS_TO_PROCESS = [
    {"lat": 34.5408, "lon": -112.4685, "name": "Prescott Valley, AZ"},
    # Add more locations as needed
    {"lat": 40.7128, "lon": -74.0060, "name": "New York, NY"},
    {"lat": 48.8566, "lon": 2.3522, "name": "Paris, France"},
]

# Define your Snowflake connection ID configured in Airflow UI
SNOWFLAKE_CONN_ID = 'snowflake_default' # Replace with your actual connection ID

# Define your Snowflake target details (can also be Airflow Variables)
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var="YOUR_DATABASE") # Example using Variable with default
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var="YOUR_SCHEMA")     # Example using Variable with default
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE", default_var="YOUR_WAREHOUSE") # Example using Variable with default

# Define staging and final table names
STG_TRAFFIC_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STG_TRAFFIC_DATA'
STG_WEATHER_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STG_WEATHER_DATA'
FINAL_TABLE = f'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TRAFFIC_WEATHER'


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Optional: Define a failure callback function similar to your lab_dag.py
# Ensure the 'discord_webhook' Airflow Variable is set if using this.
def discord_on_dag_failure_callback(context):
    """Callback function to send a notification on DAG failure."""
    try:
        discord_webhook = Variable.get("discord_webhook")
        dag_run = context.get('dag_run')
        task_instance = context.get('task_instance')
        message = (
            f"DAG failed: {dag_run.dag_id}\n"
            f"Task: {task_instance.task_id}\n"
            f"Run ID: {dag_run.run_id}\n"
            f"Log URL: {task_instance.log_url}" # Requires Airflow logging configured
        )
        # Using os.system for simplicity as in your example,
        # but requests library is generally preferred for HTTP calls.
        os.system(
            f"curl -H 'Content-Type: application/json' -X POST -d "
            f"'{{\"content\": \"DAG failure in {dag_run.dag_id}: {task_instance.task_id}. Check logs: {task_instance.log_url}\"}}' "
            f"{discord_webhook}"
        )
    except Exception as e:
        print(f"Error sending failure notification: {e}")


# Define the DAG using the decorator
@dag(
    dag_id='traffic_weather_elt_pipeline_decorator', # Changed ID to differentiate
    default_args=default_args,
    description='ELT pipeline for TomTom traffic and weather data to Snowflake (Decorator Style)',
    schedule=None, # Set your desired schedule here, e.g., '0 * * * *' for hourly
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['elt', 'traffic', 'weather', 'snowflake', 'decorator'],
    # on_failure_callback=discord_on_dag_failure_callback, # Uncomment to enable failure notification
)
def traffic_weather_elt_pipeline_decorator():

    @task
    def create_snowflake_staging_tables_task():
        """Creates staging tables in Snowflake if they don't exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # SQL to create staging traffic table
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
        # SQL to create staging weather table
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
        print("Creating staging tables if they don't exist...")
        hook.run(create_traffic_staging_sql)
        hook.run(create_weather_staging_sql)
        print("Staging tables checked/created successfully.")

    @task
    def create_snowflake_final_table_task():
        """Creates the final combined table in Snowflake if it doesn't exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # SQL to create the final combined table
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
        print("Creating final combined table if it doesn't exist...")
        hook.run(create_final_sql)
        print("Final combined table checked/created successfully.")

    # --- Extraction Tasks (Dynamic for each location) ---
    # These tasks will be generated dynamically by iterating through LOCATIONS_TO_PROCESS.
    # The @task decorator handles pushing the return value (file path) to XCom automatically.

    # We need a way to collect the outputs of these dynamic tasks.
    # The @task decorator makes this easier. We can pass the list of tasks
    # to a downstream task, and it can automatically pull their XComs.

    @task
    def collect_extracted_file_paths_task(traffic_files_xcom=None, weather_files_xcom=None):
        """
        Collects file paths pushed to XCom by upstream extraction tasks.
        Airflow automatically passes XComs from upstream tasks when using
        the decorator style and specifying arguments with matching names
        or by explicitly pulling. Here we rely on automatic pulling by
        passing the list of upstream tasks.
        """
        # When upstream tasks are passed as arguments to a @task decorated function,
        # Airflow automatically pulls their return values (XComs).
        # If multiple tasks are passed, the argument will be a list of their XComs.
        # We need to filter out None values in case some extractions failed.

        # traffic_files_xcom will be a list of file paths (or None) from traffic tasks
        # weather_files_xcom will be a list of file paths (or None) from weather tasks

        # Filter out None values
        valid_traffic_files = [f for f in traffic_files_xcom if f is not None]
        valid_weather_files = [f for f in weather_files_xcom if f is not None]

        print(f"Collected {len(valid_traffic_files)} valid traffic file paths.")
        print(f"Collected {len(valid_weather_files)} valid weather file paths.")

        # Return the collected lists. This will be pushed to XCom.
        return {'traffic_files': valid_traffic_files, 'weather_files': valid_weather_files}


    @task
    def transform_and_stage_data_task(extracted_files: dict):
        """
        Transforms raw data files (using collected paths) and saves them as staged Parquet files.
        Args:
            extracted_files: A dictionary like {'traffic_files': [...], 'weather_files': [...]}
                             containing lists of file paths, pulled from XCom.
        Returns:
            A tuple containing the paths to the staged traffic and weather Parquet files,
            or (None, None) if no data was processed. This is pushed to XCom.
        """
        traffic_file_paths = extracted_files.get('traffic_files', [])
        weather_file_paths = extracted_files.get('weather_files', [])

        print(f"Transforming {len(traffic_file_paths)} traffic files and {len(weather_file_paths)} weather files.")

        # Call the transformation function from your script
        staged_traffic_path, staged_weather_path = transform_and_combine_for_staging(
            traffic_file_paths=traffic_file_paths,
            weather_file_paths=weather_file_paths,
            output_folder='transformed_data' # Ensure this directory is mounted in Docker
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
            stage_name = f"@{SNOWFLAKE_SCHEMA}.%STG_DATA_STAGE" # Using schema-level stage

            # Create a temporary stage if it doesn't exist (optional, but good practice)
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
            stage_name = f"@{SNOWFLAKE_SCHEMA}.%STG_DATA_STAGE"

            # Create a temporary stage if it doesn't exist (optional)
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
        print("Performing final transformation and union in Snowflake...")
        hook.run(insert_final_sql)
        print("Final transformation and union completed successfully.")


    @task
    def truncate_snowflake_staging_tables_task():
        """Truncates staging tables after data is moved to the final table."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        print("Truncating staging tables...")
        hook.run(f"TRUNCATE TABLE {STG_TRAFFIC_TABLE};")
        hook.run(f"TRUNCATE TABLE {STG_WEATHER_TABLE};")
        print("Staging tables truncated successfully.")

    @task
    def dag_success_notification_task():
        """Task to send a success notification."""
        try:
            discord_webhook = Variable.get("discord_webhook")
            # Using os.system for simplicity as in your example
            os.system(
                "curl -H 'Content-Type: application/json' -X POST -d "
                "'{\"content\": \"Traffic and Weather ELT DAG completed successfully! 🎉\"}' "
                f"{discord_webhook}"
            )
            print("Success notification sent.")
        except Exception as e:
            print(f"Error sending success notification: {e}")


    # --- Define Task Dependencies ---

    # Create tables first
    create_stg_tables = create_snowflake_staging_tables_task()
    create_final_tbl = create_snowflake_final_table_task()

    # Extraction tasks (run in parallel) depend on staging tables being ready
    # We use a list comprehension to create the tasks dynamically
    traffic_extraction_tasks = [
        extract_traffic_data.override(task_id=f'extract_traffic_data_{location["name"].replace(" ", "_").replace(",", "").replace(".", "")}')(location_coords=location)
        for location in LOCATIONS_TO_PROCESS
    ]

    weather_extraction_tasks = [
        extract_weather_data.override(task_id=f'extract_weather_data_{location["name"].replace(" ", "_").replace(",", "").replace(".", "")}')(location_coords=location)
        for location in LOCATIONS_TO_PROCESS
    ]

    create_stg_tables >> traffic_extraction_tasks
    create_stg_tables >> weather_extraction_tasks

    # Collect file paths from all extraction tasks
    # We pass the lists of upstream tasks to the collection task.
    # Airflow's decorator magic will automatically pull their XComs.
    collected_files = collect_extracted_file_paths_task(
        traffic_files_xcom=traffic_extraction_tasks,
        weather_files_xcom=weather_extraction_tasks
    )

    # Transformation depends on collecting all file paths
    staged_files = transform_and_stage_data_task(extracted_files=collected_files)

    # Loading tasks depend on the transformation task completing and providing staged file paths
    # We pass the tuple of staged file paths to the loading task.
    load_to_snowflake = load_staged_data_to_snowflake_task(staged_file_info=staged_files)

    # Final transformation/union depends on the loading task completing
    final_transform = transform_and_union_in_snowflake_task()

    # Truncate staging tables after final transformation/union
    truncate_stg_tables = truncate_snowflake_staging_tables_task()

    # Success notification depends on truncation
    success_notification = dag_success_notification_task()

    # Define the overall flow
    [create_stg_tables, create_final_tbl] >> [traffic_extraction_tasks, weather_extraction_tasks]
    [traffic_extraction_tasks, weather_extraction_tasks] >> collected_files
    collected_files >> staged_files
    staged_files >> load_to_snowflake
    load_to_snowflake >> final_transform
    final_transform >> truncate_stg_tables
    truncate_stg_tables >> success_notification


# Instantiate the DAG
traffic_weather_elt_pipeline_decorator()
