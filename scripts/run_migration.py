"""Tek seferlik migration runner — 001_web_tables.sql"""
import os, sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT", "5432"),
    dbname=os.getenv("PG_DATABASE"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    connect_timeout=10,
)
print("Baglanti OK")

sql_path = Path(__file__).parent / "sql" / "001_web_tables.sql"
sql = sql_path.read_text(encoding="utf-8")

cur = conn.cursor()
cur.execute(sql)
conn.commit()
print("SQL calistirildi")

cur.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_name LIKE 'web_%' ORDER BY table_name"
)
tables = [r[0] for r in cur.fetchall()]
print("Tablolar:", tables)
conn.close()
