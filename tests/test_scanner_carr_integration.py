"""scanner.py compute_carr_stage entegrasyon testi (KARAR #733 - Paket 272).

Migration 009 sonucu eklenen 5 kolon (carr_stage, carr_stage_label,
carr_slope_pct_per_year, carr_ma_value, carr_price_vs_ma_pct) scanner.py'da:
1. ALTER TABLE listesinde mevcut (idempotent kolon ekleme)
2. INSERT statement'ta mevcut (yazılır)
3. ON CONFLICT DO UPDATE bloğunda mevcut (re-scan üzere yazılır)
4. compute_carr_stage import edilmiş ve VALUES tuple'a paslanmış

Bu test gerçek DB / yfinance bağımı OLMADAN, scanner.py kaynak metnini
parse ederek statik kontrol yapar. Manifesto Özellik #8 vibe-coding
güvencesi: AI yorumundan bağımsız doğrulama.
"""

import re
from pathlib import Path

import pytest

SCANNER_PATH = Path(__file__).parent.parent / "scanner.py"
MIGRATION_009_PATH = (
    Path(__file__).parent.parent / "scripts" / "sql" / "009_carr_stage_columns.sql"
)

# Migration 009'da eklenen 5 kolon
CARR_COLUMNS = [
    "carr_stage",
    "carr_stage_label",
    "carr_slope_pct_per_year",
    "carr_ma_value",
    "carr_price_vs_ma_pct",
]


@pytest.fixture(scope="module")
def scanner_source() -> str:
    return SCANNER_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def migration_009_source() -> str:
    return MIGRATION_009_PATH.read_text(encoding="utf-8")


class TestMigration009File:
    """Migration 009 SQL dosyası — 5 ADD COLUMN IF NOT EXISTS satırı."""

    def test_dosya_mevcut(self):
        assert MIGRATION_009_PATH.exists(), "Migration 009 dosyası bulunamadı"

    @pytest.mark.parametrize("col", CARR_COLUMNS)
    def test_kolon_alter_table_var(self, migration_009_source, col):
        pattern = rf"ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS\s+{col}\b"
        assert re.search(pattern, migration_009_source), (
            f"Migration 009'da {col} ADD COLUMN satırı yok"
        )

    def test_idempotent_pattern(self, migration_009_source):
        """Tüm ALTER TABLE satırları IF NOT EXISTS içermeli (idempotent).

        Yorum satırlarındaki 'IF NOT EXISTS' atfı sayılmaz — sadece
        gerçek ALTER TABLE satırlarındaki ADD COLUMN IF NOT EXISTS.
        """
        alter_lines = [
            ln for ln in migration_009_source.splitlines()
            if ln.strip().startswith("ALTER TABLE")
        ]
        assert len(alter_lines) == 5, f"5 ALTER TABLE bekliyor, {len(alter_lines)} bulundu"
        for ln in alter_lines:
            assert "IF NOT EXISTS" in ln, f"ALTER TABLE satırında IF NOT EXISTS yok: {ln}"


class TestScannerImport:
    """scanner.py compute_carr_stage import zinciri."""

    def test_compute_carr_stage_import(self, scanner_source):
        assert "compute_carr_stage" in scanner_source

    def test_quanfina_math_import_satiri(self, scanner_source):
        # quanfina_math import bloğunda compute_carr_stage var.
        # Yorum içindeki parantezler (örn. "(Migration 007)") regex'i bozar,
        # bu yüzden manuel split ile kesin sınır al.
        marker = "from quanfina_math import ("
        idx = scanner_source.find(marker)
        assert idx >= 0, "quanfina_math import bloğu bulunamadı"
        # Bloğun kapanışı ilk satır başı ')' karakteri
        rest = scanner_source[idx + len(marker):]
        end = rest.find("\n)")
        assert end > 0, "Import bloğu kapanışı bulunamadı"
        import_block = rest[:end]
        assert "compute_carr_stage" in import_block, (
            f"compute_carr_stage import bloğunda yok. Blok: {import_block!r}"
        )


class TestScannerAlterTableEntegrasyonu:
    """scanner.py ALTER TABLE listesi — Migration 009 kolonları orada da olmalı.

    Sebep: scanner.py ilk çalıştırmada CREATE TABLE + ALTER TABLE idempotent
    çağrılır (DB yeni kurulduğunda Migration manuel uygulanmazsa scanner
    çağrısı kolonu otomatik yaratır).
    """

    @pytest.mark.parametrize("col", CARR_COLUMNS)
    def test_alter_table_kolon_listesinde(self, scanner_source, col):
        pattern = (
            rf'"ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS\s+{col}'
        )
        # En az 1 kez var olmalı (2 ALTER bloğunda da olabilir)
        matches = re.findall(pattern, scanner_source)
        assert len(matches) >= 1, (
            f"scanner.py ALTER TABLE listesinde {col} yok — Migration 009 kayıp"
        )


class TestScannerInsertEntegrasyonu:
    """scanner.py INSERT INTO minervini_scans bloğu — 5 kolon yazılıyor mu?"""

    @pytest.fixture(scope="class")
    def insert_block(self, scanner_source):
        # En geniş INSERT bloğu (kolonlar + VALUES + ON CONFLICT)
        match = re.search(
            r"INSERT INTO minervini_scans\s*\n\s*\(.+?DO UPDATE SET.+?(?=\n\s+\"\"\"|\Z)",
            scanner_source,
            re.DOTALL,
        )
        assert match is not None, "INSERT bloğu bulunamadı"
        return match.group(0)

    @pytest.mark.parametrize("col", CARR_COLUMNS)
    def test_insert_kolon_listesinde(self, insert_block, col):
        assert col in insert_block, (
            f"INSERT INTO minervini_scans bloğunda {col} yok"
        )

    @pytest.mark.parametrize("col", CARR_COLUMNS)
    def test_on_conflict_excluded_var(self, insert_block, col):
        # ON CONFLICT DO UPDATE SET içinde {col} = EXCLUDED.{col} bekleniyor
        pattern = rf"{col}\s*=\s*EXCLUDED\.{col}"
        assert re.search(pattern, insert_block), (
            f"ON CONFLICT DO UPDATE SET içinde {col} = EXCLUDED.{col} yok"
        )

    def test_values_yer_tutucu_sayisi_eslesti(self, insert_block):
        """VALUES (%s,%s,...) yer tutucu sayısı INSERT kolon listesi ile eşit."""
        # Kolon listesi: ilk parantez içeriği
        kolon_match = re.search(
            r"INSERT INTO minervini_scans\s*\n\s*\((.+?)\)\s*VALUES",
            insert_block,
            re.DOTALL,
        )
        assert kolon_match is not None
        kolonlar = [k.strip() for k in kolon_match.group(1).split(",")]
        kolon_sayisi = len(kolonlar)

        # VALUES (%s,...) içindeki %s sayısı
        values_match = re.search(r"VALUES\s*\(([^)]+)\)", insert_block)
        assert values_match is not None
        placeholder_count = values_match.group(1).count("%s")

        assert kolon_sayisi == placeholder_count, (
            f"Kolon sayısı ({kolon_sayisi}) != yer tutucu sayısı ({placeholder_count})"
        )


class TestScannerHesapBlogu:
    """scanner.py compute_carr_stage hesap bloğu — 150 gün eşiği + try/except."""

    def test_hesap_blogu_var(self, scanner_source):
        # compute_carr_stage çağrısı (variable assignment context)
        pattern = r"compute_carr_stage\s*\("
        matches = re.findall(pattern, scanner_source)
        # En az 2 referans: import + çağrı
        assert len(matches) >= 1, (
            "compute_carr_stage fonksiyon çağrısı yok"
        )

    def test_150_gun_esigi(self, scanner_source):
        # Yetersiz veri koruması — len(closes_list) >= 150
        assert ">= 150" in scanner_source or ">=150" in scanner_source, (
            "150 gün minimum eşik kontrolü yok (compute_carr_stage ma_window=150)"
        )

    def test_try_except_korumasi(self, scanner_source):
        # carr_stage hesap try/except içinde olmalı (yetersiz veri Exception
        # bazı durumlarda atabilir — None fallback sağlanmalı)
        scan_section = scanner_source[
            scanner_source.find("KARAR #733") : scanner_source.find("Kural 3: MA200")
        ]
        assert "try:" in scan_section and "except Exception" in scan_section, (
            "carr_stage hesap bloğu try/except koruması altında değil"
        )
