import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', '5432')
DB_NAME     = os.getenv('DB_NAME', 'flightdb')
DB_USER     = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'flights.sql')


def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def create_schema():
    with open(SCHEMA_FILE) as f:
        sql = f.read()
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print("Schema created successfully.")
    except psycopg2.Error as e:
        print(f"DB error: {e.pgcode} - {e.pgerror}")
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_schema()
