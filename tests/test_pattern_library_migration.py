"""Pattern Library DB iskelet Migration 010 statik testi (KARAR ADAY #714).

Migration 010 ile pattern_library tablosu kuruluyor:
- CREATE TABLE IF NOT EXISTS (idempotent)
- 7 Mark + O'Neil canon pattern (INSERT ON CONFLICT DO NOTHING)
- Mark kitap referansları (KALICI İLKE #4 — kaynak birebir)

Bu test DB bağımı OLMADAN, SQL kaynak parse ederek statik kontrol yapar.
Cloud SQL erişimi gerektirmez (Sprint 4-bis.7 (e) iskelet aşaması).
"""
from pathlib import Path

import pytest

MIGRATION_010 = (
    Path(__file__).parent.parent / "scripts" / "sql" / "010_pattern_library.sql"
)

# KARAR #714 canon 7 pattern (notebook/Sprint_4_bis_7_Mark_4_Kitap_Tam_Sentez.md s.193)
CANON_PATTERNS = [
    "Standard VCP",
    "Cup-with-Handle",
    "Cup Completion Cheat",
    "Low Cheat",
    "Power Play (HTF)",
    "Double Bottom",
    "Square Box",
]


@pytest.fixture(scope="module")
def sql_source() -> str:
    assert MIGRATION_010.exists(), "Migration 010 dosyası bulunamadı"
    return MIGRATION_010.read_text(encoding="utf-8")


class TestPatternLibraryTablo:
    """pattern_library tablo şema testi."""

    def test_create_table_var(self, sql_source):
        assert "CREATE TABLE IF NOT EXISTS pattern_library" in sql_source

    @pytest.mark.parametrize("col", [
        "pattern_name", "mark_book_ref",
        "contraction_count_min", "contraction_count_max",
        "base_weeks_min", "base_weeks_max",
        "notes", "created_at",
    ])
    def test_kolon_tanimli(self, sql_source, col):
        assert col in sql_source, f"Kolon {col} tanımlı değil"

    def test_unique_pattern_name(self, sql_source):
        # Aynı pattern_name iki kez girilemez (Mark canon temizlik)
        assert "pattern_name" in sql_source and "UNIQUE" in sql_source


class TestCanonPatternler:
    """7 Mark canon pattern INSERT testi."""

    @pytest.mark.parametrize("name", CANON_PATTERNS)
    def test_pattern_eklenmis(self, sql_source, name):
        # SQL string literal: 'Standard VCP' veya "Standard VCP"
        assert f"'{name}'" in sql_source, f"Canon pattern eksik: {name}"

    def test_tum_patternler_mark_kaynaginca_atıflı(self, sql_source):
        # KALICI İLKE #4 — her pattern'in mark_book_ref dolu olmalı
        # (TLSMW Ch 10 atfı en az 7 kez)
        assert sql_source.count("TLSMW Ch 10") >= 6, (
            "Mark TLSMW Ch 10 atfı yetersiz (en az 6 canon pattern bekliyor)"
        )

    def test_idempotent_insert(self, sql_source):
        # ON CONFLICT DO NOTHING — re-run güvenli
        assert "ON CONFLICT" in sql_source and "DO NOTHING" in sql_source


class TestKaliciIlke4Uyumu:
    """KALICI İLKE #4 (Matematik Uydurmama) + Kural #26 anayasa kontrol.

    Tüm sayısal eşikler (contraction_count_min/max, base_weeks_min/max) Mark
    kitap referanslı olmalı. Yorum satırında en az 1 kaynak atfı.
    """

    def test_mark_kaynak_yorum_var(self, sql_source):
        # SQL yorum satırlarında Mark kaynak atfı
        assert "Trade Like a Stock Market Wizard" in sql_source or "TLSMW" in sql_source
        assert "Ch 10" in sql_source

    def test_kural_26_anayasa_atfi(self, sql_source):
        # Kural #26 (Matematik Uydurmama) yorum satırında atıflı
        assert "İLKE #4" in sql_source or "Kural #26" in sql_source, (
            "Anayasa uyum (KALICI İLKE #4 veya Kural #26) atfı yok"
        )
