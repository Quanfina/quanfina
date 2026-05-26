"""
Migration runner — Paket 166 (26 May 2026) gelistirilmis

Kullanim:
    python scripts/run_migration.py                            # default 001_web_tables.sql
    python scripts/run_migration.py 002_add_tight_low_vol_pass.sql
    python scripts/run_migration.py --all                      # scripts/sql/ TUM .sql sirayla
    python scripts/run_migration.py --all --dry-run            # SQL'i parse et + plan goster (uygulama)
    python scripts/run_migration.py --check                    # mevcut DB kolon durumu raporu
    python scripts/run_migration.py 008_trade_plan_required_fields.sql --dry-run

Idempotent disiplin (KARAR #461, 19 May 2026):
- Tum migration'lar `ADD COLUMN IF NOT EXISTS` kullanir
- NULL default — mevcut satirlar bozulmaz
- Tekrar calistirma guvenli

Paket 166 yenilikler:
- --dry-run: SQL parse + plan, DB'ye dokunmadan (offline calisir)
- --check: minervini_scans + web_trades + web_watchlist + web_signals kolon listesi
- connect_timeout 10s -> 30s (Cloud SQL paused durumlar icin)
- Temiz hata mesajlari: timeout / authentication / db ismi ayrimi
"""
import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
import psycopg2
from psycopg2 import OperationalError

SQL_DIR = Path(__file__).parent / "sql"
TIMEOUT_SEC = 30  # P166: 10 -> 30, Cloud SQL paused durumlar icin


def parse_ddl_statements(sql: str) -> list[str]:
    """SQL icindeki statement'lari cikar (yorum hariç). --dry-run icin."""
    lines = []
    for line in sql.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines)
    return [s.strip() for s in body.split(";") if s.strip()]


def dry_run_one(sql_filename: str) -> None:
    """SQL parse + statement listesi goster, DB'ye dokunmadan."""
    sql_path = SQL_DIR / sql_filename
    if not sql_path.exists():
        print(f"[HATA] {sql_path} bulunamadi")
        sys.exit(1)
    sql = sql_path.read_text(encoding="utf-8")
    statements = parse_ddl_statements(sql)
    print(f"\n=== [DRY-RUN] {sql_filename} ===")
    print(f"  {len(statements)} statement bulundu:")
    for i, stmt in enumerate(statements, 1):
        first_line = re.sub(r"\s+", " ", stmt)[:100]
        print(f"    {i}. {first_line}{'...' if len(stmt) > 100 else ''}")


def run_one(sql_filename: str, conn) -> None:
    """SQL'i DB'de uygula + dogrulama sorgusu."""
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
    # Generic dogrulama: ilgili sembolik kolonlari listele (P166 genisletildi)
    cur.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_name IN ('minervini_scans', 'web_trades') "
        "  AND column_name IN ("
        "    'tight_low_vol_pass', 'price_volume_history',"
        "    'vcp_quality_score', 'vcp_ready_score', 'power_play_pass',"
        "    'tennis_ball_pattern', 'volume_asymmetry_ratio', 'volume_asymmetry_tier',"
        "    'plan_entry_trigger', 'plan_stop', 'plan_target',"
        "    'plan_size_pct', 'plan_exit_strategy', 'plan_time_horizon'"
        "  )"
        " ORDER BY table_name, column_name"
    )
    rows = cur.fetchall()
    if rows:
        print("  Ilgili kolonlar (DB state):")
        for tbl, col, dtype in rows:
            print(f"    - {tbl}.{col}: {dtype}")


def check_schema(conn) -> None:
    """Paket 166: Mevcut DB schema durumu raporu (--check)."""
    cur = conn.cursor()
    print("\n=== DB Schema Check ===")
    for tbl in ("minervini_scans", "web_trades", "web_watchlist", "web_signals"):
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (tbl,),
        )
        rows = cur.fetchall()
        if not rows:
            print(f"\n  [{tbl}]: TABLO YOK")
            continue
        print(f"\n  [{tbl}] ({len(rows)} kolon):")
        for col, dtype in rows:
            print(f"    - {col}: {dtype}")


def connect_db():
    """Cloud SQL baglanti — 30s timeout + temiz hata mesajlari."""
    host = os.getenv("PG_HOST")
    if not host:
        print("[HATA] .env PG_HOST tanimsiz")
        sys.exit(2)
    try:
        conn = psycopg2.connect(
            host=host,
            port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            connect_timeout=TIMEOUT_SEC,
        )
        print(f"Baglanti OK ({host})")
        return conn
    except OperationalError as e:
        msg = str(e)
        print(f"[HATA] DB baglanti basarisiz ({TIMEOUT_SEC}s timeout):")
        msg_lower = msg.lower()
        if "timeout" in msg_lower or "timed out" in msg_lower or "connection refused" in msg_lower:
            print("  Olasi sebep: Cloud SQL instance paused veya IP whitelist eski")
            print("  Cozum: GCP Console -> SQL -> instance durum kontrol")
            print("         + Authorized Networks'e mevcut IP'yi ekle")
        elif "password" in msg.lower() or "authentication" in msg.lower():
            print("  Olasi sebep: PG_USER veya PG_PASSWORD yanlis")
        elif "database" in msg.lower() and "exist" in msg.lower():
            print("  Olasi sebep: PG_DATABASE adi yanlis")
        else:
            print(f"  Detay: {msg[:200]}")
        sys.exit(3)


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    check_mode = "--check" in args
    args = [a for a in args if a not in ("--dry-run", "--check")]

    # --check: schema raporu (DB baglanti gerek)
    if check_mode:
        conn = connect_db()
        check_schema(conn)
        conn.close()
        return

    # --dry-run: DB baglanti GEREKMEZ (offline SQL parse)
    if dry_run:
        target = args[0] if args else "001_web_tables.sql"
        if target == "--all":
            for path in sorted(SQL_DIR.glob("*.sql")):
                dry_run_one(path.name)
        else:
            dry_run_one(target)
        print("\n[DRY-RUN] Hicbir statement uygulanmadi.")
        return

    # Normal mod: DB'de uygula
    target = args[0] if args else "001_web_tables.sql"
    conn = connect_db()
    if target == "--all":
        for path in sorted(SQL_DIR.glob("*.sql")):
            run_one(path.name, conn)
    else:
        run_one(target, conn)
    conn.close()
    print("\n[OK] Migration tamam.")


if __name__ == "__main__":
    main()
