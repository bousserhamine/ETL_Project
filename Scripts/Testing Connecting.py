"""import psycopg2

conn = psycopg2.connect(dbname='airflow', user='airflow', password='airflow')
print("Connected successfully")
conn.close()"""
import psycopg2

try:
    conn = psycopg2.connect(
        host="",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {str(e)}")