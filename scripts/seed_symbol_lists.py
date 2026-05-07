"""
seed_symbol_lists.py - Sprint 4.3b test verisi

Sn. Ferit'in user_id=1 hesabina 4 listeye dagitilmis test hisseleri ekler.
Tum hisseler minervini_scans tablosunda mevcut (scan_date=2026-05-06).

Calistirma:
    venv\\Scripts\\python.exe scripts\\seed_symbol_lists.py

Geri alma:
    venv\\Scripts\\python.exe -c "from db_connection import get_connection; conn=get_connection(); cur=conn.cursor(); cur.execute(\\"DELETE FROM symbol_lists WHERE user_id=1 AND strategy='minervini'\\"); conn.commit(); print('Silindi:', cur.rowcount); conn.close()"

UNIQUE constraint: (user_id, symbol, list_type, strategy)
Tekrar calistirilirsa ON CONFLICT DO NOTHING ile duplicate olusmaz.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_connection import get_connection


SEED_DATA = [
    # (symbol, list_type, note)
    ("AMD",   "watch",   "Sprint 4.3b seed - Grade A, RS 90"),
    ("GOOGL", "watch",   "Sprint 4.3b seed - Grade B, RS 72"),
    ("AVGO",  "watch",   "Sprint 4.3b seed - Grade B, RS 63"),
    ("AAPL",  "watch",   "Sprint 4.3b seed - Grade C, RS 31"),
    ("NVDA",  "on_deck", "Sprint 4.3b seed - Grade A, RS 46"),
    ("AMZN",  "on_deck", "Sprint 4.3b seed - Grade B, RS 42"),
    ("GOOGL", "focus",   "Sprint 4.3b seed - promotion test"),
    ("AMD",   "buy",     "Sprint 4.3b seed - en yuksek skor"),
]

USER_ID = 1
STRATEGY = "minervini"


def main():
    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for symbol, list_type, note in SEED_DATA:
        cur.execute(
            "INSERT INTO symbol_lists (user_id, symbol, list_type, strategy, note) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id, symbol, list_type, strategy) DO NOTHING",
            (USER_ID, symbol, list_type, STRATEGY, note)
        )
        if cur.rowcount > 0:
            inserted += 1
            print("  EKLENDI: {:<6} -> {}".format(symbol, list_type))
        else:
            skipped += 1
            print("  ATLANDI: {:<6} -> {} (zaten var)".format(symbol, list_type))

    conn.commit()

    cur.execute(
        "SELECT list_type, COUNT(*) FROM symbol_lists "
        "WHERE user_id=%s AND strategy=%s GROUP BY list_type ORDER BY list_type",
        (USER_ID, STRATEGY)
    )
    rows = cur.fetchall()

    print("\n=== Sonuc ===")
    print("Eklenen: {} | Atlanan: {}".format(inserted, skipped))
    print("\nListe dagilimi:")
    for list_type, count in rows:
        print("  {:<10} : {} hisse".format(list_type, count))

    conn.close()


if __name__ == "__main__":
    main()
