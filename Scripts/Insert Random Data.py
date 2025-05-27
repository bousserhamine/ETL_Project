import psycopg2
from faker import Faker
import random

# Initialize Faker instance to generate random data
fake = Faker()

# Connect to PostgreSQL database
def get_db_connection():
    conn = psycopg2.connect(
        dbname="airflow",  # Replace with your database name
        user="airflow",      # Replace with your database username
        password="airflow",  # Replace with your password     
    )
    return conn

# Function to generate and insert random data
def insert_random_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Creating a sample table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        product_name VARCHAR(255),
        product_code VARCHAR(50),
        price DECIMAL,
        quantity INTEGER,
        date_added TIMESTAMP
    );
    """)
    conn.commit()

    # Generate and insert 100 random rows
    for _ in range(100):
        product_name = fake.company()
        product_code = fake.uuid4()
        price = round(random.uniform(10, 500), 2)
        quantity = random.randint(1, 1000)
        date_added = fake.date_this_decade()

        cursor.execute("""
        INSERT INTO stock_data (product_name, product_code, price, quantity, date_added)
        VALUES (%s, %s, %s, %s, %s);
        """, (product_name, product_code, price, quantity, date_added))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("100 rows of random data inserted successfully")

# Call the function to insert data
insert_random_data()
