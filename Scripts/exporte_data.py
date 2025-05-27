import pandas as pd
import psycopg2
import os
from datetime import datetime

def download_data():
    """Exporte chaque table PostgreSQL dans un fichier Excel séparé avec timestamp."""
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    # Liste des tables à exporter
    tables = [
        "Full_Transformation",
        "brigade",
        "portefeuille_empef",
        "terrain",
        "phonning"
    ]

    # Créer le répertoire si nécessaire
    output_dir = "/opt/airflow/raw_data/Cleaned"
    os.makedirs(output_dir, exist_ok=True)

    # Générer un timestamp unique pour tous les fichiers
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Exporter chaque table dans un fichier Excel séparé
    for table in tables:
        query = f'SELECT * FROM "{table}";'
        df = pd.read_sql_query(query, conn)

        filename = f'exported_data_{table}_{timestamp}.xlsx'
        output_path = os.path.join(output_dir, filename)

        df.to_excel(output_path, index=False)
        print(f'{table} : {output_path}')

    conn.close()

if __name__ == "__main__":
    download_data()
