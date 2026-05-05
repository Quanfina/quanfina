# Quanfina — Claude Code Bağlam Dosyası

## Proje Özeti
Kişisel trade tracking ve hisse tarama platformu. Streamlit arayüzü, PostgreSQL (Google Cloud SQL) veritabanı. Minervini PA1 tarama motoru, pozisyon yönetimi, risk hesaplama, trade journal.

## Teknoloji Yığını
- **Frontend:** Streamlit (çok sayfalı uygulama, `pages/` dizini)
- **Veritabanı:** PostgreSQL (Google Cloud SQL) — `db_connection.py` üzerinden psycopg2 + SQLAlchemy
- **Scanner backend:** Flask (`scanner_server.py`) — Cloud Run / Cloud Scheduler
- **Test:** pytest, savepoint/rollback stratejisi (gerçek PostgreSQL, veri kalmaz)
- **Python ortamı:** `venv/` — `venv\Scripts\python.exe`

## Dizin Yapısı
```
c:\Projeler\Quanfina\
├── app.py                     # Streamlit ana sayfa
├── db_connection.py           # PostgreSQL CRUD — tüm DB fonksiyonları burada
├── trade_journal.py           # TradeJournal class — trades/legs/exits/stops CRUD
├── quanfina_guards.py         # PLANLANMADI — @requires dekoratörü (bir sonraki sprint)
├── scanner.py                 # Minervini PA1 tarama motoru
├── scanner_server.py          # Flask wrapper (Cloud Run)
├── styles.py                  # Tasarım sistemi (tokenlar, CSS, bileşenler)
├── database.py                # LEGACY SQLite — dokunma, sadece referans
├── migrate_to_postgres.py     # SQLite→PG migration, sadece scanner tabloları
├── requirements.txt           # Production bağımlılıkları
├── requirements-dev.txt       # Dev: pytest>=8.0
├── tests/
│   ├── __init__.py
│   └── test_trade_journal.py  # 50 test, savepoint/rollback fixture
└── pages/
    ├── 1_Genel_Bakis.py
    ├── 2_Piyasa_Durumu.py
    ├── 3_Minervini.py
    ├── 4_Carr.py
    ├── 5_Tum_Sinyaller.py
    ├── 6_Yeni_Pozisyon.py     # Trade entry form + risk calculator
    ├── 7_Pozisyonlar.py       # Pozisyon yönetimi (CRUD — edit/close/delete)
    ├── 8_Portfoy_Risk.py      # Portfolio risk analizi
    ├── 9_Trade_Journal.py     # Serbest metin journal (journal_entries tablosu)
    ├── 10_istatistikler.py
    └── 11_Sektor_Rotasyonu.py # RS sektör rotasyonu
```

## PostgreSQL Şeması

### Temel tablolar
| Tablo | Açıklama |
|---|---|
| `trades` | Pozisyonlar — 40 kolon (27 orijinal + 13 yeni) |
| `trade_legs` | Multi-leg pyramiding kayıtları |
| `trade_exits` | Kısmi/tam çıkış kayıtları |
| `stop_history` | Trailing stop tarihçesi |
| `journal_entries` | Serbest metin trader günlüğü |
| `portfolios` | Portföy başlangıç/mevcut değerleri |
| `minervini_scans` | PA1 tarama sonuçları |
| `minervini_52w_high` | 52 haftalık yüksek verileri |
| `sector_rotation` | Sektör RS skorları |

### trades tablosu — önemli kolon kararları
- `trade_type` VARCHAR ('Long'/'Short') — eski kolon, mevcut kod kullanıyor, silinmedi
- `invest_type` SMALLINT (1=LONG, 2=SHORT) — yeni kolon, TradeJournal API'si bunu kullanıyor
- `portfolio_id` INTEGER FK → `portfolios` — normalize yapı korundu (`portfolio TEXT` eklenmedi)
- `notes` TEXT (plural) — mevcut naming korundu (`note` singular eklenmedi)
- `status` VARCHAR: 'Open' / 'Closed' / 'Deleted' — büyük harf convention, değiştirilmedi
- `entry_date` / `exit_date` TIMESTAMP — DATE'e dönüştürülmedi, mevcut veri korundu
- `deleted_at` TIMESTAMP — soft delete için

### Yabancı anahtar davranışları
- `trade_legs.trade_id` → `trades.id` ON DELETE CASCADE
- `trade_exits.leg_id` → `trade_legs.id` ON DELETE CASCADE
- `stop_history.trade_id` → `trades.id` ON DELETE CASCADE
- `journal_entries.linked_trade_id` → `trades.id` ON DELETE SET NULL (constraint: `fk_journal_trade`)

### Schema init fonksiyonu
```python
from db_connection import init_trade_journal_tables
init_trade_journal_tables()  # idempotent, tekrar çalıştırılabilir
```

## Bağlantı Yönetimi
```python
# Doğrudan bağlantı (psycopg2)
from db_connection import get_connection
conn = get_connection()

# SQLAlchemy engine (Pandas için)
from db_connection import get_engine
engine = get_engine()
```
Ortam değişkenleri: `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_PORT`

## TradeJournal API
```python
from trade_journal import TradeJournal

# Production kullanım
with TradeJournal() as tj:
    tid = tj.add_trade('NVDA', invest_type=1, entry_date=..., entry_price=120.0, stop_loss=115.0, quantity=100)
    lid = tj.add_leg(tid, shares=100, price=120.0, leg_date=...)
    tj.update_stop(tid, new_stop_price=118.0, reason='trailing')
    result = tj.close_trade(tid, exit_date=..., exit_price=130.0)

# Test kullanımı (external conn — commit yapılmaz)
conn = get_connection()
tj = TradeJournal(conn=conn)
# ... işlemler ...
conn.rollback()  # veri temizlenir
conn.close()
```

**Transaction kuralı:** `TradeJournal(conn=external)` → class commit çağırmaz, caller yönetir.

## Testler
```powershell
# Tüm testler
venv\Scripts\python.exe -m pytest tests\test_trade_journal.py -v

# Kapsam raporu
venv\Scripts\python.exe -m pytest tests\test_trade_journal.py --cov=trade_journal
```
Test stratejisi: gerçek PostgreSQL + savepoint/rollback — veri DB'de kalmaz.

## Streamlit Çalıştırma
```powershell
venv\Scripts\python.exe -m streamlit run app.py
```

## Önemli Kurallar ve Kısıtlar
1. `database.py` (SQLite legacy) ve `migrate_to_postgres.py`'a **dokunma** — sadece referans
2. Mevcut tablolarda kolon **DROP etme** — sadece ADD COLUMN veya CREATE TABLE
3. **Markets 360 referansı verme** (clean-room uyumu) — Vd, Wp, valueGetter, aB() gibi isimler docstring'e girmesin
4. Yeni runtime bağımlılığı ekleme (psycopg2, SQLAlchemy zaten var)
5. `updated_at` Python tarafında `NOW()` ile yönetilir — TRIGGER yok
6. Status değerleri büyük harf: 'Open', 'Closed', 'Deleted' (lowercase'e geçiş yapılmadı)

## Sonraki Sprint'ler (Planlanmış)
- `quanfina_guards.py` — `@requires` dekoratörü, MPA_Karakutu refactor hazırlığı
  - MISSING sentinel, resolve_field, has_required_fields, requires(*fields, default=None)
  - tests/test_quanfina_guards.py ile birlikte (50 test planlandı)
- Trade Journal UI — `9_Trade_Journal.py` yeniden yazımı, TradeJournal class'ını kullanacak
