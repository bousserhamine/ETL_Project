import pandas as pd
import psycopg2
from psycopg2 import extras

# Load the CSV file
csv_file_path = r'/opt/airflow/raw_data/SPP.csv'
df = pd.read_csv(csv_file_path, sep=';', usecols=['num_dos', 'Flag'])

# Database connection parameters
conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='postgres',
    port='5432'
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Drop table if exists and create new one
cur.execute("DROP TABLE IF EXISTS SPP;")
cur.execute("""
    CREATE TABLE SPP (
        num_dos TEXT,
        Flag TEXT
    );
""")

# Insert DataFrame into the table using execute_values (fast & efficient)
extras.execute_values(
    cur,
    "INSERT INTO SPP (num_dos, Flag) VALUES %s",
    df.values.tolist()
)

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

print("Data successfully loaded into PostgreSQL table 'SPP'")
