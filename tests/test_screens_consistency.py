"""
UI ↔ Backend tutarlılık testi — Kural #24 Aşama 5 (Pytest)

Vibe coding güvencesi: AI yorumundan bağımsız renkler.
22 May 2026 — Sn. Ferit'in 11 koşullu Trend Template UI'de eksik kalan
hacim filtresini kendi hafızasıyla yakalaması paterni → bu test kalıcı
koruma.

Test edilen tutarlılık:
  - UI listesi (web/types/screens.ts SCREEN_CONDITIONS["stage2_10p"])
  - Backend Finviz URL filter (scanner.py get_finviz_screener)
  - SQL filter (api/db_helpers.py SCREENS_READY_9["stage2_10p"])
"""

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent.parent


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def _extract_screen_conditions_count(ts_source: str, slug: str) -> int:
    """SCREEN_CONDITIONS[slug] içindeki { source: ... } sayısını döndürür."""
    pattern = rf'{slug}:\s*\[(.*?)\]\s*,\s*'
    m = re.search(pattern, ts_source, re.DOTALL)
    if not m:
        pytest.fail(f"SCREEN_CONDITIONS['{slug}'] block bulunamadı")
    block = m.group(1)
    return len(re.findall(r'\{\s*source:', block))


def _extract_finviz_filters(scanner_source: str) -> list[str]:
    """get_finviz_screener içindeki filters listesini döndürür."""
    pattern = r'def get_finviz_screener.*?filters\s*=\s*",".join\(\[(.*?)\]\)'
    m = re.search(pattern, scanner_source, re.DOTALL)
    if not m:
        pytest.fail("get_finviz_screener filters list bulunamadı")
    block = m.group(1)
    return re.findall(r'"([^"]+)"', block)


def _extract_sql_filter(db_helpers_source: str, slug: str) -> str:
    """SCREENS_READY_9[slug]["filter"] string'ini döndürür."""
    pattern = rf'"{slug}":\s*\{{[^}}]*"filter":\s*"([^"]+)"'
    m = re.search(pattern, db_helpers_source)
    if not m:
        pytest.fail(f"SCREENS_READY_9['{slug}'] filter bulunamadı")
    return m.group(1)


class TestStage2_10pConsistency:
    """stage2_10p (Trend Template) — UI ↔ Backend ↔ SQL bire-bir uyum."""

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def scanner(self) -> str:
        return _read("scanner.py")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_ui_condition_count_matches_documented(self, ts):
        """UI'de 11 koşul gösterilmeli (2 Quanfina + 8 Mark + 1 Mark Ekstra).
        Sıralama (22 May 2026 Sn. Ferit talimat):
        - 1-2: Quanfina Ek (Fiyat $10 + Hacim 500K, evren daraltma)
        - 3-10: Mark Resmi Kural (kitap birebir 8 madde, Trade Like a Wizard s.79)
        - 11: Mark Ekstra Kural (Mark kitap tavsiyesi, turuncu)
        """
        n = _extract_screen_conditions_count(ts, "stage2_10p")
        assert n == 11, (
            f"UI condition count = {n}, beklenen 11 "
            f"(2 Quanfina + 8 Mark Resmi + 1 Mark Ekstra). "
            f"22 May 2026 Sn. Ferit talimat sıralama."
        )

    def test_finviz_url_has_volume_filter(self, scanner):
        """Backend Finviz URL'inde sh_avgvol_o500 (hacim ≥ 500K) olmalı."""
        filters = _extract_finviz_filters(scanner)
        assert "sh_avgvol_o500" in filters, (
            "Backend hacim filtresi (sh_avgvol_o500) atlanmış. "
            "Mark Resmi Kural evren daraltma — UI ve backend bire-bir."
        )

    def test_finviz_url_has_price_filter(self, scanner):
        """Backend Finviz URL'inde sh_price_o10 (fiyat ≥ $10) olmalı."""
        filters = _extract_finviz_filters(scanner)
        assert "sh_price_o10" in filters

    def test_finviz_url_has_8_dma_filters(self, scanner):
        """Backend 5 SMA filtresi + 2 52W filtresi (toplam Mark Resmi Kural 7 filter)."""
        filters = _extract_finviz_filters(scanner)
        required = [
            "ta_sma50_pa", "ta_sma200_pa",
            "ta_sma50_sa150", "ta_sma50_sa200", "ta_sma150_sa200",
            "ta_highlow52w_a25h", "ta_highlow52w_b75l",
        ]
        for f in required:
            assert f in filters, f"Mark Resmi Kural filter eksik: {f}"

    def test_sql_filter_has_rs_threshold(self, db_helpers):
        """SQL filter rs_ibd >= 70 (KARAR #484) içermeli."""
        sql = _extract_sql_filter(db_helpers, "stage2_10p")
        assert "rs_ibd >= 70" in sql, (
            f"SQL filter Mark Resmi Kural RS≥70 (Koşul 9) içermiyor. "
            f"Mevcut: {sql!r}"
        )

    def test_sql_filter_has_price_threshold(self, db_helpers):
        """SQL filter price >= 10 (Quanfina Ek) içermeli."""
        sql = _extract_sql_filter(db_helpers, "stage2_10p")
        assert "price >= 10" in sql

    def test_sql_filter_has_passed_check(self, db_helpers):
        """SQL filter passed = 1 (MA200 slope) içermeli."""
        sql = _extract_sql_filter(db_helpers, "stage2_10p")
        assert "passed = 1" in sql

    def test_ui_mark_kural_count(self, ts):
        """UI'de tam 8 Mark Resmi Kural olmalı (kitap birebir 8 madde).
        Trade Like a Wizard s.79 — RS Rating 8. resmi madde (dahil).
        Kitap 1. maddesi "Fiyat > 150 ve 200 DMA" tek madde (UI ayırma yok).
        """
        pattern = r'stage2_10p:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        mark_count = block.count('source: "mark"')
        assert mark_count == 8, (
            f"Mark Resmi Kural sayısı = {mark_count}, beklenen 8 "
            f"(Trade Like a Wizard, s.79, 8 madde — RS Rating 8. madde dahil)."
        )

    def test_ui_quanfina_ek_count(self, ts):
        """UI'de tam 2 Quanfina Ek Filtre olmalı (Fiyat + Hacim)."""
        pattern = r'stage2_10p:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        quanfina_count = block.count('source: "quanfina"')
        assert quanfina_count == 2, (
            f"Quanfina Ek Filtre sayısı = {quanfina_count}, beklenen 2 "
            f"(Fiyat ≥ $10 + Hacim ≥ 500K)."
        )

    def test_ui_mark_ekstra_count(self, ts):
        """UI'de tam 1 Mark Ekstra Kural olmalı (turuncu — kitap tavsiyesi).
        22 May 2026 Sn. Ferit talimat: 11. madde Mark Ekstra Kural.
        Mark Resmi Kural ZORUNLU değil ama tercih (preferably/ideally şartları).
        """
        pattern = r'stage2_10p:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        ekstra_count = block.count('source: "mark_ekstra"')
        assert ekstra_count == 1, (
            f"Mark Ekstra Kural sayısı = {ekstra_count}, beklenen 1 "
            f"(Mark kitap tavsiyesi — RS ideali 80-90+)."
        )


class TestTemelElemeConsistency:
    """temel_eleme (Mark Fundamental ZORUNLU) — UI ↔ Backend bire-bir uyum.

    23 May 2026 — Sn. Ferit talimat: "bu 5 koşulu yapalım trend template gibi"
    KARAR ADAY #486: Temel Eleme şablonu 5 koşul (2 Quanfina Ek + 3 Mark Fundamental).
    """

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def scanner(self) -> str:
        return _read("scanner.py")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_ui_condition_count_is_5(self, ts):
        """temel_eleme UI'de tam 5 koşul gösterilmeli.
        Sn. Ferit talimat (23 May 2026): "bu 5 koşulu yapalım trend template gibi"
        Dağılım: 2 Quanfina Ek + 3 Mark Resmi (Fundamental ZORUNLU).
        """
        n = _extract_screen_conditions_count(ts, "temel_eleme")
        assert n == 5, (
            f"temel_eleme UI condition count = {n}, beklenen 5 "
            f"(2 Quanfina Ek + 3 Mark Fundamental ZORUNLU)."
        )

    def test_ui_quanfina_ek_count_is_2(self, ts):
        """temel_eleme UI'de tam 2 Quanfina Ek olmalı (Fiyat + Hacim — evren daraltma)."""
        pattern = r'temel_eleme:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        quanfina_count = block.count('source: "quanfina"')
        assert quanfina_count == 2, (
            f"Quanfina Ek sayısı = {quanfina_count}, beklenen 2 "
            f"(Fiyat ≥ $10 + Hacim ≥ 500K)."
        )

    def test_ui_mark_resmi_count_is_3(self, ts):
        """temel_eleme UI'de tam 3 Mark Resmi olmalı (Fundamental ZORUNLU).
        Kitap birebir:
          - EPS Q/Q ≥ %25 (TLSMW s.127)
          - Sales Q/Q ≥ %25 (TLSMW s.132)
          - ROE ≥ %15-17 (Momentum Masters s.74)
        """
        pattern = r'temel_eleme:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        mark_count = block.count('source: "mark"')
        assert mark_count == 3, (
            f"Mark Resmi (Fundamental) sayısı = {mark_count}, beklenen 3 "
            f"(EPS Q/Q + Sales Q/Q + ROE — Mark Fundamental ZORUNLU)."
        )

    def test_ui_no_mark_ekstra(self, ts):
        """temel_eleme'de Mark Ekstra OLMAMAlı (Fundamental zorunlu, tavsiye yok).
        Trend Template'den farklı: Power Play'de var ama Temel Eleme'de yok.
        """
        pattern = r'temel_eleme:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        ekstra_count = block.count('source: "mark_ekstra"')
        assert ekstra_count == 0, (
            f"Mark Ekstra sayısı = {ekstra_count}, beklenen 0 "
            f"(Temel Eleme'de tavsiye yok, hepsi ZORUNLU)."
        )

    def test_backend_scanner_has_roe_filter(self, scanner):
        """Backend get_finviz_fundamental_only fonksiyonunda fa_roe_pos15 olmalı.
        23 May 2026 yeni ekleme — Mark MM s.74 birebir: "ROE of 15-17% or higher".
        Önceki sürümde sadece EPS Q/Q + Sales Q/Q vardı, ROE filtresi YOKtu.
        """
        assert "fa_roe_pos15" in scanner, (
            "Backend ROE filtresi (fa_roe_pos15) get_finviz_fundamental_only'da "
            "bulunamadı. Mark Fundamental ZORUNLU 3. eşik (Momentum Masters s.74)."
        )

    def test_backend_scanner_has_eps_qoq_filter(self, scanner):
        """Backend get_finviz_fundamental_only'da fa_epsqoq_o25 olmalı."""
        assert "fa_epsqoq_o25" in scanner, (
            "Backend EPS Q/Q filtresi (fa_epsqoq_o25) eksik. "
            "Mark Fundamental ZORUNLU 1. eşik (TLSMW s.127)."
        )

    def test_backend_scanner_has_sales_qoq_filter(self, scanner):
        """Backend get_finviz_fundamental_only'da fa_salesqoq_o25 olmalı."""
        assert "fa_salesqoq_o25" in scanner, (
            "Backend Sales Q/Q filtresi (fa_salesqoq_o25) eksik. "
            "Mark Fundamental ZORUNLU 2. eşik (TLSMW s.132)."
        )

    def test_db_helpers_has_temel_eleme_slug(self, db_helpers):
        """api/db_helpers.py SCREENS_READY_9'da temel_eleme slug olmalı."""
        assert '"temel_eleme":' in db_helpers, (
            "SCREENS_READY_9 dict'inde temel_eleme slug bulunamadı."
        )

    def test_db_helpers_temel_eleme_uses_fundamental_table(self, db_helpers):
        """temel_eleme slug'unun table override'ı minervini_fundamental_only olmalı."""
        # temel_eleme entry'sini bul
        pattern = r'"temel_eleme":\s*\{[^}]*"table":\s*"([^"]+)"'
        m = re.search(pattern, db_helpers)
        assert m is not None, "temel_eleme entry'de table field bulunamadı"
        table = m.group(1)
        assert table == "minervini_fundamental_only", (
            f"temel_eleme table override = '{table}', beklenen 'minervini_fundamental_only'."
        )
