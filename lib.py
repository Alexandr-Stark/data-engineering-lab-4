import csv
import os
import psycopg2
from psycopg2 import sql
from pathlib import Path

entry_db_config = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

db_config = {
    'host': 'localhost',
    'database': 'data_engineering',
    'user': 'postgres',
    'password': 'postgres'
}

def check_if_data_eng_db_exists(conn):
    db_name = db_config.get("database")
    db_owner = db_config.get("user")
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"""
                CREATE DATABASE {db_name}
                    WITH
                    OWNER = {db_owner}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False;
                           """)
            
def get_connection():
    entry_conn = psycopg2.connect(**entry_db_config)
    check_if_data_eng_db_exists(entry_conn)
    conn = psycopg2.connect(**db_config)
    return conn

def process_data(table_name: str, conn):
    setup_table_by_name(table_name, conn)
    insert_data_from_csv(table_name, conn)

def setup_table_by_name(table_name: str, conn):
    schema_path = f"schema/{table_name}.sql"
    does_schema_exist = os.path.isfile(schema_path)
    if not does_schema_exist:
        print(f"Сталася помилка: таблиці '{table_name}' не існує.")
    
    print(f"Завантаження скрипта '{table_name}.sql' та створення нової таблиці...")
    ddl_script = Path(schema_path).read_text()
    with conn.cursor() as cursor:
        cursor.execute(ddl_script)
    conn.commit()
    cursor.close()
    print(f"Таблиця '{table_name}' створено'")

def insert_data_from_csv(table_name: str, conn):
    data_path = f"data/{table_name}.csv"
    does_data_exist = os.path.isfile(data_path)
    if not does_data_exist:
        print(f"Сталася помилка: дані з таблиці '{table_name}' не існують.")

    print(f"Завантаження даних з файлу '{table_name}.csv' і додавання в базу даних...")
    with conn.cursor() as cursor:
        with open(data_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)

            cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name)))

            cleaned_up_rows = [[value.strip() for value in row] for row in csv_reader]
            
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(lambda col: sql.Identifier(col.strip()), header)),
                sql.SQL(", ").join(sql.Placeholder() * len(header)),
            )

            print(insert_query)

            cursor.executemany(insert_query, cleaned_up_rows)
        
    conn.commit()
    cursor.close()
    print(f"Дані додані в таблицю '{table_name}'")
            


