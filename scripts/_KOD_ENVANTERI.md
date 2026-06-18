# 📂 Quanfina — Kod Envanteri (Aşama 1.C)

> **Amacı:** Mevcut kod tabanının tam haritası. Aşama 1.D (Streamlit
> Emeklilik) ve 1.E (Final Harita) kararlarına girdi. Yaşayan kaynak —
> kod değiştikçe ilgili satır güncellenir (Kural #8 + #11).
>
> **Sınıflandırma:**
> - 🟢 **Yaşayan** — aktif kullanım, production veya planlı geleceği var
> - 🟡 **Kararsız** — şu an aktif ama strateji belirsiz, ileride karar
> - 🔴 **Emekli adayı** — Next.js karşılığı var, silmeye/arşive hazır
> - ⚫ **Legacy/Referans** — dokunma, sadece tarihsel
> - ⚪ **Scratch/Yetim** — ne için olduğu belirsiz, temizlik hedefi

**Son tarama:** 18 Mayıs 2026 (Aşama 1.E + 2 hijyen turu sonrası: build_index v2.1, hesap_tarama, Kural #16)

---

## 🟢 YAŞAYAN — Kök Python (production)

| Dosya | Satır | KB | Görev | Bağlantı |
|---|---:|---:|---|---|
| `db_connection.py` | 998 | 46.6 | PostgreSQL CRUD ana — psycopg2 + SQLAlchemy | Tüm Python tarafı + api/main.py |
| `quanfina_math.py` | **1367** | **~59** | Matematik motoru + Brandon VCP + VCP Quality + Inside/Outside + Ready Score + Power Play (Sprint 4-bis.4-5) | tests/test_quanfina_math.py **170 test PASS** |
| `scanner.py` | **1955** | **~90** | Minervini PA1 scanner + 28 kolon (Migration 001-006) + Finviz Drift Guard + PVH OHLC lookback 80g | Cloud Run: quanfina-scanner |
| `scanner_server.py` | 144 | 5.7 | Flask wrapper (scanner'ı HTTP'ye açar) | Cloud Run + Cloud Scheduler |
| `trade_journal.py` | 473 | 19.5 | TradeJournal class — trades/legs/exits/stops CRUD | tests/test_trade_journal.py 50 test PASS |

---

## 🟢 YAŞAYAN — api/ FastAPI

| Dosya | Satır | KB | Görev | Not |
|---|---:|---:|---|---|
| `api/main.py` | 1129 | 54.2 | FastAPI router (watchlist/trades/signals/health) | Mono-file, ileride bölünebilir |
| `api/db_helpers.py` | 212 | 8.5 | DB helper'lar (web tabloları için) | web_watchlist, web_trades |

---

## 🟢 YAŞAYAN — web/ Next.js 16

**Dizin yapısı:**
```
web/
├── app/
│   ├── layout.tsx (34) — root
│   └── (dashboard)/
│       ├── layout.tsx (15)
│       ├── page.tsx (88) — ana sayfa
│       ├── api-test/page.tsx (111)
│       ├── carr/page.tsx (139) ⭐ Streamlit'te boştu — burada dolduruldu
│       ├── hisse/[symbol]/page.tsx — TradingView chart sayfası
│       ├── journal/page.tsx (196)
│       ├── minervini/page.tsx (106)
│       ├── piyasa-durumu/page.tsx (68)
│       ├── signals/page.tsx (156) ⭐ Streamlit'te boştu — burada dolduruldu
│       └── watchlist/page.tsx (216)
├── components/ — shadcn/ui + özel
├── hooks/
├── lib/
├── styles/
├── types/
├── public/
├── AGENTS.md ⚠️ İncele
├── CLAUDE.md ⚠️ web/'in kendi bağlam dosyası — kök CLAUDE.md ile senkron mu?
├── components.json — shadcn config
├── next.config.ts
├── package.json + pnpm-lock.yaml
└── tsconfig.json
```

**Önemli not:** `web/CLAUDE.md` ayrı bir bağlam dosyası — kök
`CLAUDE.md` anayasa katmanından bağımsız. Aşama 1.E (Final Harita)
öncesi ikisi arasındaki ilişki netleştirilmeli.

---

## 🟢 YAŞAYAN — scripts/

| Dosya | Satır | Görev | Durum |
|---|---:|---|---|
| `scripts/notebook_yedekle.ps1` | 90 | Notebook ZIP yedekleme (rotation 10) | ✅ 17 May 2026 — Manifesto Özellik #9 günlük ritüel |
| `scripts/sizma_kontrol.ps1` | 148 | Push öncesi sızma kontrolü (6/6 PASS gerek) | ✅ pre-push hook ile canlı, 19+ canlı test |
| `scripts/build_index.ps1` | ~210 | Master indeks doğrulayıcı + üretici (v2.1) | ✅ Aşama 1.E.c — 18 May v2.1 backup wildcard |
| `scripts/hesap_tarama.ps1` | — | Hesap matrisi otomatik keşif (Kural #12) | ✅ Aşama 1 sonrası hijyen |
| `scripts/run_migration.py` | 28 | DB migration runner | Aktif |
| `scripts/seed_initial_data.py` | 107 | İlk seed verisi | Aktif |
| `scripts/seed_symbol_lists.py` | 61 | Symbol list seed | Aktif |

---

## 🟢 YAŞAYAN — tests/ (gerçek PG + savepoint/rollback)

| Dosya | Satır | Test sayısı | Görev |
|---|---:|---:|---|
| `tests/test_quanfina_math.py` | 641 | 122 ✅ | Matematik motoru testleri |
| `tests/test_trade_journal.py` | 375 | 50 ✅ | TradeJournal CRUD testleri |
| `tests/__init__.py` | 0 | — | pytest paket markeri |

---

## ⚫ ARŞIVDE — Aşama 1.D ile taşındı (17 May 2026)

**13 Streamlit pages** + **3 altyapı** + **1 scratch** = `_archive/` altında (22 dosya, 3,676 satır toplam).

### `_archive/streamlit_legacy/pages/`
| Eski yer | Satır | Next.js karşılığı | Commit |
|---|---:|---|---|
| `pages/1_Genel_Bakis.py` | 66 | `web/app/(dashboard)/page.tsx` | `fed11c7` (1.D.2) |
| `pages/2_Piyasa_Durumu.py` | 86 | `web/app/(dashboard)/piyasa-durumu/page.tsx` | `fed11c7` |
| `pages/2_Screens.py` | 192 | (yok) | `f13d18b` (1.D.3) |
| `pages/3_Minervini.py` | 373 | `web/app/(dashboard)/minervini/page.tsx` | `fed11c7` |
| `pages/3_Minervini_old.py` | 723 | "_old" — ölü | `a9f307d` (1.D.1) |
| `pages/4_Carr.py` | 7 | `web/app/(dashboard)/carr/page.tsx` | `a9f307d` |
| `pages/5_Tum_Sinyaller.py` | 7 | `web/app/(dashboard)/signals/page.tsx` | `a9f307d` |
| `pages/6_Yeni_Pozisyon.py` | 310 | (yok) | `f13d18b` |
| `pages/7_Pozisyonlar.py` | 511 | (yok) | `f13d18b` |
| `pages/8_Portfoy_Risk.py` | 7 | (yok) | `a9f307d` |
| `pages/9_Trade_Journal.py` | 328 | `web/app/(dashboard)/journal/page.tsx` | `fed11c7` |
| `pages/10_istatistikler.py` | 246 | (yok) | `f13d18b` |
| `pages/11_Sektor_Rotasyonu.py` | 237 | (yok) | `f13d18b` |

### `_archive/streamlit_legacy/` (altyapı)
| Eski yer | Satır | Görev | Commit |
|---|---:|---|---|
| `app.py` | 20 | Streamlit giriş | `4078263` (1.D.4) |
| `styles.py` | 358 | Streamlit tasarım sistemi | `4078263` |
| `database.py` | 46 | SQLite legacy (eski 1_Genel_Bakis kullanıcısı) | `4078263` |

### `_archive/scratch/`
| Eski yer | Satır | Tip | Commit/Yöntem |
|---|---:|---|---|
| `_list_cols.py` | 159 | Tracked scratch | `ecd3944` (1.D.5, git mv) |
| `test_finviz_sectors.py` | 26 | Ignore'da scratch | (fiziksel mv) |
| `test_pg.py` | 22 | Ignore'da scratch | (fiziksel mv) |
| `test_sector_match.py` | 85 | Ignore'da scratch | (fiziksel mv) |
| `test_sectors_full.py` | 26 | Ignore'da scratch | (fiziksel mv) |

**Sonraki adım:** "Tamamen silme" zamanlaması Aşama 5'te değerlendirilir (Sn. Ferit kararı: "sonradan tamamen silinecek"). Şu anki haliyle git history'de korunuyor, ihtiyaç olursa `git log --follow _archive/.../<dosya>` ile bakılır.

---

## ⚫ LEGACY/REFERANS — Aktif ama dokunma (CLAUDE.md kuralı)

| Dosya | Satır | Sebep |
|---|---:|---|
| `migrate_to_postgres.py` | 157 | SQLite→PG migration, sadece scanner tabloları için. CLAUDE.md "dokunma" |

---

## ⚠️ DİKKAT NOKTALARI (sürekli izlenir)

### ✅ Çözüldü
1. ~~**`web/CLAUDE.md` var**~~ — 18 May 2026 kontrol: sadece `@AGENTS.md` import, senkron sorunu yok. Kök CLAUDE.md ile çelişki YOK.
2. ~~**`web/AGENTS.md` var**~~ — Next.js breaking changes uyarısı (CLAUDE.md Dizin Yapısı'nda atıf). Bağımsız bağlam dosyası, sorun değil.
3. ~~**3 boş Streamlit sayfası**~~ — Aşama 1.D'de `_archive/streamlit_legacy/`'e taşındı (commit `a9f307d`).

### ⏳ Açık (sürekli)
1. **`api/main.py` 1129 satır mono-file** — Aşama 2+ router'lara bölünebilir (öncelik düşük, çalışıyor)
2. **`scanner.py` 1955 satır** — Cloud Run prod, dokunma ama dokümante et (Sprint 4-bis.5+ büyüdü, refactor Sprint 4-bis.7+ kuyruğa erteleme)
3. **`quanfina_math.py` 1367 satır** — Matematik motoru (Sprint 4-bis.4-5 zirve: +627 satır VCP + Quality + Inside/Outside + Ready + Power Play). 170 pytest PASS

---

## 📊 Özet Sayısallar (Sprint 4-bis.5 zirve sonrası — 20 May 2026 ~05:00)

| Kategori | Dosya | Toplam Satır | Not |
|---|---:|---:|---|
| 🟢 Yaşayan (Python kök) | 6 | **5,327** | scanner +339 / math +627 (Sprint 4-bis.4-5) |
| 🟢 Yaşayan (FastAPI) | 2 | **2,024** | tight_low_volume + 4 yeni screen + DB 503 handler |
| 🟢 Yaşayan (Next.js web — sayfalar) | 11+ | ~1,300+ | VCP Quality + Ready + HTF cellRenderer |
| 🟢 Yaşayan (scripts) | **17** | **3,234** | +pasif_tara + hijyen_paketi + drive_sync v2.3 + saglik_kontrol v0.5.1 + build_index v2.1 |
| 🟢 Yaşayan (tests) | 3 | **1,788** | 170 pytest PASS |
| ⚫ Legacy referans (migrate_to_postgres) | 1 | 157 | |
| ⚫ Arşiv (`_archive/streamlit_legacy/` Aşama 1.D) | 16 | 3,517 | |
| ⚫ Arşiv (`_archive/scratch/` ignore'da fiziksel) | 5 | 318 | |

**Yaşayan toplam:** **~13,700 satır** (Aşama 1.D öncesi ~10,650 → Sprint 4-bis.5 sonrası).
**Sprint 4-bis büyüme:** ~+5,700 satır (Brandon VCP + Drift Guard + UI hijyen + Skeleton + scanner kolonları).
**Arşivde:** ~3,835 satır.
**Yaşayan kod tabanı azalması:** ~2,650 satır (Streamlit çıkışı).

---

## 🕸️ Dependency Haritası (Adım 1.C.d)

### Python tarafı

**Merkez node: `db_connection.py`** (8 import eden)
```
db_connection.py ← scanner.py
                ← trade_journal.py
                ← api/main.py
                ← scripts/seed_symbol_lists.py
                ← pages/3_Minervini.py
                ← pages/6_Yeni_Pozisyon.py
                ← pages/7_Pozisyonlar.py
                ← pages/9_Trade_Journal.py
                ← pages/10_istatistikler.py
                ← pages/11_Sektor_Rotasyonu.py
                ← tests/test_trade_journal.py
                ← test_sectors_full.py (scratch)
                ← test_sector_match.py (scratch)
```

**`quanfina_math.py`** (5 import eden)
```
quanfina_math.py ← db_connection.py (kullanıyor)
                 ← pages/3_Minervini.py
                 ← pages/6_Yeni_Pozisyon.py
                 ← pages/7_Pozisyonlar.py
                 ← pages/9_Trade_Journal.py
                 ← pages/10_istatistikler.py
                 ← tests/test_quanfina_math.py
```

**`styles.py`** (8 Streamlit pages/ import)
```
styles.py ← app.py + pages/1-11 (Streamlit tarafı)
```
🔴 **Streamlit emeklilik (Aşama 1.D) sonrası ölecek** — styles.py
hiçbir non-Streamlit dosyada kullanılmıyor.

**`database.py`** (SQLite legacy)
```
database.py ← pages/1_Genel_Bakis.py (TEK kullanıcı)
```
⚫ Sadece bir Streamlit sayfası kullanıyor. 1_Genel_Bakis emekli
edilirse database.py tamamen ölü kod olur.

**`scanner.py`**
```
scanner.py ← scanner_server.py (Cloud Run wrapper)
           ← test_sectors_full.py (scratch)
```

**`trade_journal.py`**
```
trade_journal.py ← tests/test_trade_journal.py
```
Production'da TradeJournal class API'sı şu an Streamlit pages/ tarafından
kullanılmıyor — sadece test'te. Aşama 1.D'de pages/9_Trade_Journal Next.js
journal/page.tsx'e geçince api/main.py bunu sarmalayabilir.

### Web ↔ FastAPI tarafı

**Next.js HTTP istemciler (`web/hooks/`):**
```
api/main.py (port 8000)
   ↑ HTTP
   ├── web/hooks/use-watchlist.ts → /api/watchlist (GET)
   ├── web/hooks/use-watchlist-mutations.ts → /api/watchlist (POST/PATCH/DELETE)
   ├── web/hooks/use-trades.ts → /api/trades
   ├── web/hooks/use-signals.ts → /api/signals
   └── web/app/(dashboard)/api-test/page.tsx → /api/health (test)
```

**Proxy:** `web/next.config.ts` Next.js → FastAPI proxy (8000 port)
ayarıyor.

### Cloud Run akışı

```
Cloud Scheduler (cron)
    │
    ▼
Cloud Run: quanfina-scanner
    │
    ├── scanner_server.py (Flask HTTP)
    │       │
    │       └── scanner.py (Minervini PA1)
    │              │
    │              └── db_connection.py → Cloud SQL PostgreSQL
    │
    └── PostgreSQL'e minervini_scans, minervini_52w_high yazar
```

### Aşama 1.D Etki Analizi

| Streamlit dosyası emekli olunca | Birlikte ölen |
|---|---|
| `app.py` + tüm `pages/*.py` | `styles.py` (sadece Streamlit kullanıyor) |
| `pages/1_Genel_Bakis.py` | `database.py` (sadece bu kullanıyor) |
| Streamlit tarafı tamamen | `test_finviz_sectors.py`, `test_sector_*.py` (scratch'ler) |

**Aşama 1.D sonrası kalacak Python tarafı:**
- `db_connection.py`, `quanfina_math.py`, `trade_journal.py` (core)
- `scanner.py`, `scanner_server.py` (Cloud Run)
- `api/main.py`, `api/db_helpers.py` (FastAPI)
- `scripts/*` (utility)
- `tests/test_*.py` (test suite)

**Kazanım:** ~3,500 satır Streamlit ölü kod arşive, kalan kod tabanı daha temiz.

---

## 🛠️ Bu dosya nasıl bakım yapılır

- Yeni dosya eklendiğinde uygun kategoriye yazılır
- Streamlit dosyası emekliye alındığında 🟡/🔴'dan ⚫ (arşiv) kategorisine geçer
- Aşama 1.D başlarken: 🔴 ve ⚪ dosyalar `_archive/streamlit_legacy/`'e taşınır
- Aşama 1.E'de: bu dosyaya `notebook/_INDEX.md` master indeksinden bağlantı verilir
- Satır sayısı/boyut otomatik tarama scripti (`scripts/envanter_tara.ps1`) ileride yazılabilir (Aşama 5)

---

## 📚 İlgili referanslar

- `notebook/_ROADMAP.md` → Aşama 1.C → 5 plan
- `notebook/Notebook_C1_Sprint_QuickStart.md` → Kod sprint planı (kodlamaya dönüş için)
- `notebook/Notebook_A_Vizyon.md` → AÇIK KONU #36, #37, #38 (boş sayfa, eski kod)
- `CLAUDE.md` → Dizin Yapısı + Kodlama Standartları + Bilgi Haritası
