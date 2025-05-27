from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Répertoire contenant les fichiers à surveiller
WATCHED_DIR = '/opt/airflow/raw_data' # remplacez par le bon chemin
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

    # Si nouveaux fichiers trouvés, on retourne True
    if new_files:
        print(f"Nouveaux fichiers détectés : {new_files}")
        return True
    else:
        print("Aucun nouveau fichier détecté.")
        return False

def run_extraction_script(**kwargs):
    """Exécute le script d'extraction si des fichiers sont détectés."""
    script_path = '/opt/airflow/Scripts/insert_excel_to_postgres2.py'  # modifiez le chemin
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    print("Résultat du script d’extraction :")
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Erreur d'exécution : {result.stderr}")

with DAG(
    dag_id='detect_and_extract_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 21),
    schedule_interval='@monthly',
    catchup=True
) as dag:

    detect_task = PythonOperator(
        task_id='detect_new_files',
        python_callable=detect_new_files,
    )

    extract_task = PythonOperator(
        task_id='run_extraction_script',
        python_callable=run_extraction_script,
    )

    detect_task >> extract_task
