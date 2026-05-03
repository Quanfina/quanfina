"""
SQLite → PostgreSQL (Google Cloud SQL) migration script.
Kullanım: python migrate_to_postgres.py
Gereksinim: pip install psycopg2-binary python-dotenv pandas
"""

import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "quanfina.db")

PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "sslmode":  "require",
}

TABLES = [
    "minervini_scans",
    "minervini_fundamental_scans",
    "minervini_fundamental_only",
    "minervini_52w_high",
    "minervini_watchlist",
]

UNIQUE_CONSTRAINTS = {
    "minervini_scans":             "UNIQUE (scan_date, ticker)",
    "minervini_fundamental_scans": "UNIQUE (scan_date, ticker)",
    "minervini_fundamental_only":  "UNIQUE (scan_date, ticker)",
    "minervini_52w_high":          "UNIQUE (scan_date, ticker)",
}

TYPE_MAP = {
    "TEXT":    "TEXT",
    "REAL":    "DOUBLE PRECISION",
    "INTEGER": "INTEGER",
    "NUMERIC": "DOUBLE PRECISION",
    "BLOB":    "BYTEA",
    "":        "TEXT",
}

BATCH_SIZE = 500


def pg_type(sqlite_type: str) -> str:
    return TYPE_MAP.get(sqlite_type.upper().strip(), "TEXT")


def get_sqlite_columns(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    rows = cursor.fetchall()
    # rows: (cid, name, type, notnull, dflt_value, pk)
    return rows


def build_create_table(table, columns):
    col_defs = []
    for cid, name, col_type, notnull, dflt, pk in columns:
        if pk and col_type.upper() == "INTEGER":
            col_defs.append(f'    "{name}" SERIAL PRIMARY KEY')
        else:
            pg_t = pg_type(col_type)
            not_null = " NOT NULL" if notnull and not pk else ""
            default = f" DEFAULT {dflt}" if dflt is not None and not pk else ""
            col_defs.append(f'    "{name}" {pg_t}{not_null}{default}')

    if table in UNIQUE_CONSTRAINTS:
        col_defs.append(f"    {UNIQUE_CONSTRAINTS[table]}")

    return f'CREATE TABLE IF NOT EXISTS "{table}" (\n' + ",\n".join(col_defs) + "\n);"


def migrate_table(sqlite_conn, pg_conn, table):
    print(f"\n{'='*50}")
    print(f"Tablo: {table}")

    columns = get_sqlite_columns(sqlite_conn, table)
    if not columns:
        print(f"  [UYARI] SQLite'ta '{table}' tablosu bulunamadı, atlanıyor.")
        return 0, 0

    ddl = build_create_table(table, columns)
    print(f"  DDL oluşturuldu ({len(columns)} kolon)")

    pg_cur = pg_conn.cursor()
    pg_cur.execute(ddl)
    pg_conn.commit()
    print(f"  [OK] PostgreSQL'de tablo oluşturuldu / zaten mevcut")

    df = pd.read_sql_query(f'SELECT * FROM "{table}"', sqlite_conn)
    sqlite_count = len(df)
    print(f"  SQLite kayıt sayısı: {sqlite_count}")

    if sqlite_count == 0:
        print(f"  [ATLA] Tablo boş.")
        return 0, 0

    col_names = [c[1] for c in columns]
    placeholders = ",".join(["%s"] * len(col_names))
    col_list = ",".join([f'"{c}"' for c in col_names])
    insert_sql = (
        f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT DO NOTHING'
    )

    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[col_names].itertuples(index=False, name=None)
    ]

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        psycopg2.extras.execute_batch(pg_cur, insert_sql, batch)
        inserted += len(batch)
        print(f"  ... {inserted}/{sqlite_count} satır aktarıldı", end="\r")

    pg_conn.commit()
    print(f"  [OK] {inserted} satır aktarıldı              ")

    pg_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    pg_count = pg_cur.fetchone()[0]
    pg_cur.close()

    return sqlite_count, pg_count


def main():
    print("=== QUANFINA: SQLite -> PostgreSQL Migration ===\n")

    if not os.path.exists(SQLITE_PATH):
        print(f"[HATA] SQLite dosyası bulunamadı: {SQLITE_PATH}")
        sys.exit(1)

    missing = [k for k in ("PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[HATA] .env dosyasında eksik değişkenler: {missing}")
        sys.exit(1)

    print(f"SQLite : {SQLITE_PATH}")
    print(f"PG Host: {PG_CONFIG['host']}:{PG_CONFIG['port']} / {PG_CONFIG['dbname']}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)

    print("\nPostgreSQL bağlantısı kuruluyor...")
    try:
        pg_conn = psycopg2.connect(**PG_CONFIG)
    except Exception as e:
        print(f"[HATA] PostgreSQL bağlantı hatası: {e}")
        sqlite_conn.close()
        sys.exit(1)
    print("[OK] Bağlantı başarılı\n")

    results = []
    for table in TABLES:
        try:
            sq_cnt, pg_cnt = migrate_table(sqlite_conn, pg_conn, table)
            match = "OK" if sq_cnt == pg_cnt else "UYUMSUZ"
            results.append((table, sq_cnt, pg_cnt, match))
        except Exception as e:
            print(f"  [HATA] {table}: {e}")
            pg_conn.rollback()
            results.append((table, "?", "?", "HATA"))

    sqlite_conn.close()
    pg_conn.close()

    print("\n" + "="*60)
    print("MİGRATION RAPORU")
    print("="*60)
    print(f"{'Tablo':<35} {'SQLite':>8} {'PG':>8} {'Durum':>10}")
    print("-"*60)
    all_ok = True
    for table, sq, pg, match in results:
        print(f"{table:<35} {str(sq):>8} {str(pg):>8} {match:>10}")
        if match != "OK":
            all_ok = False
    print("="*60)

    if all_ok:
        print("\n[OK] Tüm tablolar başarıyla aktarıldı.")
    else:
        print("\n[UYARI] Bazı tablolarda uyumsuzluk var. Yukarıdaki raporu kontrol et.")
        sys.exit(1)


if __name__ == "__main__":
    main()
