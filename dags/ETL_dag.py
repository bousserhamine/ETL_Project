from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import subprocess

# Répertoire contenant les fichiers à surveiller
WATCHED_DIR = '/opt/airflow/raw_data'  # remplacez par le bon chemin
LOG_FILE = '/tmp/last_seen_files.log'  # pour garder la trace des fichiers déjà traités

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def detect_new_files(**kwargs):
    """Détecte les nouveaux fichiers dans le dossier."""
    all_files = set(os.listdir(WATCHED_DIR))
    
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            old_files = set(f.read().splitlines())
    else:
        old_files = set()

    new_files = all_files - old_files

    # Log les fichiers vus
    with open(LOG_FILE, 'w') as f:
        f.write('\n'.join(all_files))

    if new_files:
        print(f"Nouveaux fichiers détectés : {new_files}")
        return True
    else:
        print("Aucun nouveau fichier détecté.")
        return False

def run_extraction_script(**kwargs):
    """Exécute les scripts d'extraction si des fichiers sont détectés."""
    script_paths = [
        '/opt/airflow/Scripts/insert_excel_to_postgres2.py',
        '/opt/airflow/Scripts/Store_CTX.py',
        '/opt/airflow/Scripts/Store_pcx.py',
        '/opt/airflow/Scripts/Store_Sinister.py',
        '/opt/airflow/Scripts/Store_Sogeconso.py',
        '/opt/airflow/Scripts/Store_SPP.py'
    ]

    for script_path in script_paths:
        print(f"Exécution du script : {script_path}")
        result = subprocess.run(['python3', script_path], capture_output=True, text=True)

        print(f"Résultat du script {script_path} :")
        print(result.stdout)

        if result.returncode != 0:
            raise Exception(f"Erreur d'exécution du script {script_path} : {result.stderr}")

def run_export_script(**kwargs):
    """Exécute le script d'export après les tests DBT."""
    script_path = '/opt/airflow/Scripts/exporte_data.py'
    print(f"Exécution du script : {script_path}")
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)

    print(f"Résultat du script {script_path} :")
    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Erreur d'exécution du script {script_path} : {result.stderr}")

with DAG(
    dag_id='Etl_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 21),
    schedule_interval='@once',
    catchup=False
) as dag:

    detect_task = PythonOperator(
        task_id='detect_new_files',
        python_callable=detect_new_files,
    )

    extract_task = PythonOperator(
        task_id='run_extraction_script',
        python_callable=run_extraction_script,
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed_all',
        bash_command="""
        cd /opt/airflow/Dbt/etl_dbt_project && \
        dbt seed --select Brigade_M_1 && \
        dbt seed --select DVR && \
        dbt seed --select phonning_m-1 
    
        """
    )

    dbt_run = BashOperator(
        task_id='dbt_run_all',
        bash_command="""
        cd /opt/airflow/Dbt/etl_dbt_project && \
        dbt run --select Full_Transformation && \
        dbt run --select portefeuille_base && \
        dbt run --select portefeuille_empef && \
        dbt run --select portefeuille && \
        dbt run --select portefeuille_with_rap_cin && \
        dbt run --select portefeuille_2 && \
        dbt run --select brigade && \
        dbt run --select phonning && \
        dbt run --select terrain
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/Dbt/etl_dbt_project && dbt test'
    )


    export_task = PythonOperator(
        task_id='run_export_script',
        python_callable=run_export_script,
    )


    # Définition des dépendances
    detect_task >> extract_task >> dbt_seed >> dbt_run >> dbt_test >> export_task


