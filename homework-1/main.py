"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os
from pathlib import Path

password = os.getenv('postgres_password')
# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password=password
)

def data_record_db(csv_file_path, table_name):
    # Create a cursor object
    cur = conn.cursor()
    # Open the CSV file and read its contents
    with open(csv_file_path, 'r') as f:
        csv_reader = csv.reader(f)
        # Define the query to insert data into the table # next(csv_reader) # skip header row
        insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(next(csv_reader)))})"

        # Iterate over the CSV rows and execute the insert query for each row
        for row in csv_reader:
            cur.execute(insert_query, row)

        # Commit the changes to the database
        conn.commit()

    # Close the cursor and connection
    cur.close()


# Define the path to the CSV file and the name of the table to load it into
csv_file_path_customers = Path("north_data", "customers_data.csv")
table_name_customers = "customers"

csv_file_path_employees_data = Path("north_data", "employees_data.csv")
table_name_employees_data = "employees"

csv_file_path_orders_data = Path("north_data", "orders_data.csv")
table_name_orders_data = "orders"

data_record_db(csv_file_path_customers, table_name_customers)
data_record_db(csv_file_path_employees_data, table_name_employees_data)
data_record_db(csv_file_path_orders_data, table_name_orders_data)
conn.close()
