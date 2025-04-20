# Dockerfile
# Custom Dockerfile for Airflow image to install dependencies

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG AIRFLOW_EXTRAS
ARG INSTALL_REQUIREMENTS

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Install necessary system packages (e.g., for database clients, git, etc.)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#   libpq-dev \ # Example for psycopg2 if not in AIRFLOW_EXTRAS
#   git \
#   && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies from requirements.txt
# Use a build argument to conditionally install requirements
RUN if [ "${INSTALL_REQUIREMENTS}" = "true" ]; then \
    pip install --no-cache-dir -r /requirements.txt ; \
    fi

# Copy your requirements file into the image
COPY requirements.txt /requirements.txt

# Copy your custom modules (extracts.py, transform.py, etc.) into the image
# This is an alternative to volume mounting the 'dags' folder,
# but volume mounting is better for development as you don't rebuild on code changes.
# If you choose to copy, uncomment these lines and adjust paths:
# COPY ./dags /opt/airflow/dags/
# COPY ./plugins /opt/airflow/plugins/

# Switch back to the airflow user
USER airflow

# The rest is handled by the base Airflow image entrypoint
