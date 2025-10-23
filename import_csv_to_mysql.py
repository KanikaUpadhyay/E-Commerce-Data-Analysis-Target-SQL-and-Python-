import pandas as pd
import pymysql as mysql  # Using PyMySQL instead of mysql.connector
import os
import sys

# Folder containing CSV files
folder_path = 'C:/Users/hp/Desktop/ECommerce'
print("DEBUG: Script started!")
print("DEBUG: Folder path is", folder_path)

try:
    files_in_folder = os.listdir(folder_path)
    print("DEBUG: Files in folder:", files_in_folder)
except Exception as e:
    print("ERROR: Could not list files in folder:", e)
    sys.exit(1)

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')
]

# Connect to MySQL using PyMySQL
try:
    conn = mysql.connect(
        host='localhost',
        user='root',
        password='krka@9702',
        database='ecommerce',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    print("DEBUG: Successfully connected to MySQL")
except mysql.MySQLError as err:
    print("ERROR: Could not connect to MySQL:", err)
    sys.exit(1)

# Function to get SQL type
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# Process each CSV
for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    if not os.path.exists(file_path):
        print(f"WARNING: {csv_file} not found in folder, skipping.")
        continue

    print(f"\n📂 Processing {csv_file}...")

    first_chunk = True

    for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=1000, low_memory=False), start=1):
        print(f"DEBUG: Reading chunk {chunk_number}, {len(chunk)} rows")

        chunk = chunk.convert_dtypes()
        chunk = chunk.where(pd.notnull(chunk), None)
        chunk.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in chunk.columns]

        if first_chunk:
            columns = ', '.join([f'`{col}` {get_sql_type(chunk[col].dtype)}' for col in chunk.columns])
            try:
                cursor.execute(f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})')
                print(f"DEBUG: Table `{table_name}` created or already exists")
            except mysql.MySQLError as err:
                print(f"ERROR: Could not create table `{table_name}`:", err)
                continue
            first_chunk = False

        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in chunk.columns])}) VALUES ({', '.join(['%s'] * len(chunk.columns))})"
        data = [tuple(None if pd.isna(x) else x for x in row) for row in chunk.to_numpy()]

        batch_size = 500
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                cursor.executemany(sql, batch)
                conn.commit()
                print(f"DEBUG: Inserted batch {i // batch_size + 1} of chunk {chunk_number} ({len(batch)} rows)")
            except mysql.MySQLError as err:
                print(f"ERROR: Inserting batch {i // batch_size + 1} of chunk {chunk_number}:", err)
                conn.rollback()

    print(f"✅ Finished loading {table_name}")

# Close connection
conn.close()
print("\n🎉 All CSV files processed!")

