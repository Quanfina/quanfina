"""
Migration runner — argumanli (Sprint 4-bis.4 ile generic'lesti).

Kullanim:
    python scripts/run_migration.py                            # default 001_web_tables.sql
    python scripts/run_migration.py 002_add_tight_low_vol_pass.sql
    python scripts/run_migration.py --all                      # scripts/sql/ altindaki TUM .sql sirayla

KARAR #461 (19 May 2026): 002_add_tight_low_vol_pass.sql idempotent
- ALTER TABLE ... ADD COLUMN IF NOT EXISTS, dolayisi ile zarar yok
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
import psycopg2

SQL_DIR = Path(__file__).parent / "sql"


def run_one(sql_filename: str, conn) -> None:
    sql_path = SQL_DIR / sql_filename
    if not sql_path.exists():
        print(f"[HATA] {sql_path} bulunamadi")
        sys.exit(1)
    sql = sql_path.read_text(encoding="utf-8")
    print(f"\n=== {sql_filename} calistiriliyor ===")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print(f"[OK] {sql_filename} basarili")
    # Generic dogrulama: tum minervini_scans kolonlarini listele
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'minervini_scans' "
        "  AND column_name IN ('tight_low_vol_pass', 'price_volume_history') "
        "ORDER BY column_name"
    )
    rows = cur.fetchall()
    if rows:
        print("Ilgili kolonlar:")
        for col, dtype in rows:
            print(f"  - {col}: {dtype}")


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "001_web_tables.sql"
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=10,
    )
    print(f"Baglanti OK ({os.getenv('PG_HOST')})")

    if target == "--all":
        for path in sorted(SQL_DIR.glob("*.sql")):
            run_one(path.name, conn)
    else:
        run_one(target, conn)

    conn.close()
    print("\n[OK] Migration tamam.")


if __name__ == "__main__":
    main()
