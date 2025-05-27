import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

EXCEL_FILE = r"C:\\Users\\dell\\Airflow_Docker\\Pro_Env\\data\\Base_test_cleaned.csv"
CHUNK_SIZE = 100

def clean_text(value):
    """Clean and ensure proper encoding for text values"""
    if isinstance(value, str):
        return value.strip().encode('utf-8', 'ignore').decode('utf-8')
    return value

def create_table(conn):
    """Create table if not exists (same as before)"""
    pass  # Keep your existing create_table logic

def process_excel_chunks():
    """Process CSV file in manual chunks"""
    conn = None
    total_rows = 0
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_table(conn)

        # Load only the header to get column names
        df_sample = pd.read_csv(EXCEL_FILE, nrows=0, sep=";")
        columns = df_sample.columns.tolist()

        # Dynamically check if expected date columns exist
        date_cols = ['dat_enr', 'ech_deb', 'ech_fin', 'dat_arr', 'date_calcul']
        existing_date_cols = [col for col in date_cols if col in columns]

        # Process file in chunks
        for chunk in pd.read_csv(EXCEL_FILE, chunksize=CHUNK_SIZE, sep=";", dtype=str, names=columns, skiprows=1):
            chunk = chunk.where(pd.notnull(chunk), None)  # Handle missing values

            # Clean text columns
            for col in chunk.select_dtypes(include=['object']).columns:
                chunk[col] = chunk[col].apply(clean_text)

            # Convert date columns (only if they exist)
            for col in existing_date_cols:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce', dayfirst=True).dt.date

            data = [tuple(x) for x in chunk.to_numpy()]

            if data:  # Insert non-empty chunks
                try:
                    with conn.cursor() as cursor:
                        execute_batch(cursor, sql.SQL("""
                            INSERT INTO client_data ({columns})
                            VALUES ({values})
                        """).format(
                            columns=sql.SQL(',').join(map(sql.Identifier, columns)),
                            values=sql.SQL(',').join([sql.Placeholder()] * len(columns))
                        ), data)
                        conn.commit()
                        total_rows += len(data)
                        logger.info(f"Processed {len(data)} rows (Total: {total_rows})")
                except Exception as chunk_error:
                    logger.error(f"Error in chunk: {str(chunk_error)}")
                    conn.rollback()

        logger.info(f"Total {total_rows} rows inserted successfully")

    except Exception as e:
        logger.error(f"Fatal error processing data: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_excel_chunks()
