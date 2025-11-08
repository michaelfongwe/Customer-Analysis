import pandas as pd
import requests
import io
import psycopg2
from psycopg2 import sql
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os


# ============================================================
# 1. LOAD ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

KOBO_USERNAME = os.getenv("Kobo_username")
KOBO_PASSWORD = os.getenv("kobo_password")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/ak3QkwKnpveHvN9VpSnHVR/export-settings/esgYaL5fz4qoW3xqHtBs4hT/data.csv"

PG_HOST = os.getenv("SQL_Host")
PG_PORT = os.getenv("SQL_Port")
PG_DATABASE = os.getenv("SQL_DATABASE")
PG_USER = os.getenv("SQL_Username")
PG_PASSWORD = os.getenv("SQL_password")

schema_name = "MyWork"
table_name = "customer_feedback"


# ============================================================
# 2. FETCH KOBO CSV DATA
# ============================================================
print("📥 Fetching data from KoboToolbox...")

response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code != 200:
    print(f"❌ Failed to fetch data: Status {response.status_code}")
    exit()

print("✅ Data fetched successfully")

csv_buffer = io.StringIO(response.text)
df = pd.read_csv(csv_buffer, sep=";", on_bad_lines="skip")


# ============================================================
# 3. CLEAN COLUMN NAMES
# ============================================================
def clean(col):
    return (
        col.strip()
           .replace(" ", "_")
           .replace("-", "_")
           .replace("/", "_")
           .replace(".", "_")
           .replace("’", "")
           .replace("â", "")
           .replace("", "")
           .replace("?", "")
           .replace("(", "")
           .replace(")", "")
    )

original_columns = df.columns.tolist()
cleaned_columns = [clean(c) for c in original_columns]
df.columns = cleaned_columns

print("✅ Columns cleaned for PostgreSQL")


# ============================================================
# 4. CONNECT TO POSTGRESQL
# ============================================================
print("🔌 Connecting to PostgreSQL...")

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)
cur = conn.cursor()

print("✅ Connected to PostgreSQL")


# ============================================================
# 5. CREATE SCHEMA (CORRECT CASE-SENSITIVE)
# ============================================================
cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS "{}"').format(sql.SQL(schema_name)))
conn.commit()

print(f"✅ Schema '{schema_name}' ensured")


# ============================================================
# 6. CREATE TABLE WITH ALL EXPLICIT COLUMNS
# ============================================================
print("🛠 Creating table with all Kobo columns...")

column_definitions = ",\n".join([
    f'"{cleaned_columns[i]}" TEXT'
    for i in range(len(cleaned_columns))
])

create_table_query = sql.SQL(f"""
CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
    id SERIAL PRIMARY KEY,
    {column_definitions}
);
""")

cur.execute(create_table_query)
conn.commit()

print(f"✅ Table '{schema_name}.{table_name}' created successfully")


# ============================================================
# 7. INSERT DATA
# ============================================================
print("📤 Inserting data into PostgreSQL...")

columns_sql = ", ".join([f'"{c}"' for c in cleaned_columns])
placeholders = ", ".join(["%s"] * len(cleaned_columns))

insert_query = f"""
INSERT INTO "{schema_name}"."{table_name}" ({columns_sql})
VALUES ({placeholders});
"""

for _, row in df.iterrows():
    cur.execute(insert_query, tuple(row))

conn.commit()
cur.close()
conn.close()

print("✅✅ ETL COMPLETED — Data loaded into PostgreSQL successfully!")
