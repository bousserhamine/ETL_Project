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
    "host": "postgres",
    "port": "5432"
}

EXCEL_FILE = "/opt/airflow/raw_data/new_base.csv"
CHUNK_SIZE = 100

def clean_text(value):
    """Clean and ensure proper encoding for text values"""
    if isinstance(value, str):
        return value.strip().encode('utf-8', 'ignore').decode('utf-8')
    return value

def create_table(conn):
    """Create the new_base table with appropriate types and primary key"""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS new_base (
                sit VARCHAR(10),
                num_dos VARCHAR(10),
                n_dos VARCHAR(20) ,
                num_cin VARCHAR(15),
                nom_pre VARCHAR(50),
                dat_enr DATE,
                csp VARCHAR(5),
                prd VARCHAR(10),
                sit_dos VARCHAR(10),
                stage VARCHAR(10),
                encours VARCHAR(20),
                prov VARCHAR(20),
                prv_cum VARCHAR(20),
                nbr_imp_initial VARCHAR(10),
                nbr_imp VARCHAR(10),
                age_imp VARCHAR(10),
                ech_deb DATE,
                ech_fin DATE,
                dur_crd VARCHAR(10),
                mt_cred VARCHAR(20),
                mensualite VARCHAR(20),
                mt_dit VARCHAR(20),
                cod_emp VARCHAR(20),
                lib_emp VARCHAR(50),
                cod_agc VARCHAR(10),
                lib_agc VARCHAR(50),
                dat_arr DATE,
                Commentaire TEXT,
                matricule VARCHAR(20),
                lib_vil VARCHAR(50),
                date_calcul DATE,
                cod_rev VARCHAR(20),
                lib_rev VARCHAR(50),
                cli_adr TEXT,
                cli_vil VARCHAR(10),
                cli_tel VARCHAR(20)
            );
        """)
        conn.commit()
        logger.info("Table 'new_base' created or already exists.")

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
                            INSERT INTO new_base ({columns})
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
