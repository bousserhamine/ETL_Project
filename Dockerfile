#Le Dockerfile ajoute à Airflow tous les paquets utiles
# Use the official Airflow image
FROM apache/airflow:2.10.5
# Switch to the airflow user before installing Python packages
USER airflow

# Define build args for constraints URL
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.10
# Install Python dependencies using the constraints file
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-postgres \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    pandas \
    openpyxl \
    python-dotenv
