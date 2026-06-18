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

    def test_ui_madde_8_52w_low_is_30_percent(self, ts):
        """UI'de "52 haftalık dipten en az %30 yukarıda" olmalı (Mark birebir).
        24 May 2026 — KALICI İLKE #4 ihlali düzeltildi: %25 → %30.
        Mark Kitap birebir: TLSMW s.79 "at least 30 percent above its 52-week low".
        """
        pattern = r'stage2_10p:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        # %30 doğru, %25 hatalı (eski metin)
        assert "dipten en az %30" in block, (
            "UI Madde 8 '52 haftalık dipten en az %30 yukarıda' beklenirken farklı. "
            "Mark TLSMW s.79 birebir: '30 percent above 52-week low'."
        )
        # %25 dipten yanlışlıkla geri eklenmemeli
        assert "dipten en az %25" not in block, (
            "UI Madde 8 eski %25 değeri tespit edildi. Mark birebir %30 olmalı "
            "(TLSMW s.79). KALICI İLKE #4 ihlali — uydurma değer YASAK."
        )

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

    def test_ui_mark_ekstra_count_is_3(self, ts):
        """temel_eleme UI'de tam 3 Mark Ekstra olmalı (Fundamental Soft Score).
        24 May 2026 revize — Çift Danışma (Gem + Quanfina Notebook):
          Mark felsefesi gereği Fundamental "Hard Filter" DEĞİL, "Soft Score".
          Yeni IPO + biotech + Story Stocks Hard Filter'a takılırsa Quanfina
          patlayıcı fırsatları eler.
          - EPS Q/Q ≥ %25 (TLSMW s.127) — mark_ekstra (turuncu)
          - Sales Q/Q ≥ %25 (TLSMW s.132) — mark_ekstra (turuncu)
          - ROE ≥ %15-17 (Momentum Masters s.74) — mark_ekstra (turuncu)
        """
        pattern = r'temel_eleme:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        ekstra_count = block.count('source: "mark_ekstra"')
        assert ekstra_count == 3, (
            f"Mark Ekstra (Fundamental Soft Score) sayısı = {ekstra_count}, beklenen 3 "
            f"(EPS Q/Q + Sales Q/Q + ROE — Mark felsefesi gereği turuncu)."
        )

    def test_ui_no_mark_resmi(self, ts):
        """temel_eleme'de Mark Resmi (yeşil) OLMAMAlı.
        24 May 2026 revize: Mark felsefesi gereği Fundamental Hard Filter değil,
        hepsi Soft Score (turuncu).
        """
        pattern = r'temel_eleme:\s*\[(.*?)\]\s*,\s*'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        mark_count = block.count('source: "mark"')
        assert mark_count == 0, (
            f"Mark Resmi sayısı = {mark_count}, beklenen 0 "
            f"(Temel Eleme'de Fundamental Hard Filter yok, hepsi Soft Score)."
        )

    def test_backend_scanner_no_roe_hard_filter(self, scanner):
        """Backend get_finviz_fundamental_only'da fa_roe_pos15 OLMAMAlı.
        24 May 2026 — Çift Danışma sonucu (Gem + Quanfina Notebook):
          Mark felsefesi gereği ROE "Hard Filter" DEĞİL, "Soft Score" (Recipe).
          ROE Hard Filter olarak uygulanırsa yeni IPO + biotech + Story Stocks
          Açıklanamayan Güç hisseleri elenir.
          ROE Finviz extras (c=33) ile DB'ye kolon olarak çekilir, UI Soft Score.
        """
        # fa_roe_pos15 kaldırıldı — Mark felsefesi gereği Soft Score
        # NOT: scanner.py'de yorumda "fa_roe_pos15 KALDIRILDI" geçebilir, sadece
        # filters listesindeki AKTİF kullanımı kontrol edilir.
        # get_finviz_fundamental_only fonksiyon içindeki filters = ",".join([...])
        # bloğuna fa_roe_pos15 string'i girmemeli.
        m = re.search(
            r'def get_finviz_fundamental_only.*?filters\s*=\s*",".join\(\[(.*?)\]\)',
            scanner, re.DOTALL,
        )
        assert m is not None, "get_finviz_fundamental_only filters list bulunamadı"
        filters_block = m.group(1)
        active_filters = re.findall(r'"([^"]+)"', filters_block)
        assert "fa_roe_pos15" not in active_filters, (
            "Backend ROE filtresi (fa_roe_pos15) get_finviz_fundamental_only filters "
            "listesinden KALDIRILMALI — Mark felsefesi gereği Soft Score, Hard Filter değil. "
            "ROE c=33 ile DB'ye kolon olarak çekilir, UI'da Mark Ekstra (turuncu)."
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


class TestTamMinerviniConsistency:
    """tam_minervini (Trend Template + Fundamentals birleşik) — Hibrit Pipeline.

    24 May 2026 — Sn. Ferit talimat: "tam minervini tarama listesini yapıcaz
    hem teknik hem temel kaynaklara danışarak".

    KARAR ADAY #495: Tam Minervini Tarama (Hibrit 15 koşul):
      AŞAMA 1 SCREEN (Hard Filter, 10): 2 Quanfina Ek + 8 Mark Resmi Teknik
      AŞAMA 2 RECIPE (Soft Score, 5):   EPS Q/Q + Sales Q/Q + ROE + Yıllık EPS + Margin

    Çift Danışma sonucu: Gem 01_Minervini_Uzmanı (kitap birebir) + Quanfina
    Notebook NotebookLM (sistem perspektifi) — Mark felsefesi tam uyum.
    """

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_ui_condition_count_is_15(self, ts):
        """tam_minervini UI'de tam 15 koşul gösterilmeli (Hibrit Pipeline).
        Dağılım: 2 Quanfina Ek + 8 Mark Resmi Teknik + 5 Mark Ekstra Soft Score.
        """
        n = _extract_screen_conditions_count(ts, "tam_minervini")
        assert n == 15, (
            f"tam_minervini UI condition count = {n}, beklenen 15 "
            f"(2 Quanfina + 8 Mark Resmi Teknik + 5 Mark Ekstra Soft Score)."
        )

    def test_ui_quanfina_ek_count_is_2(self, ts):
        """tam_minervini'de tam 2 Quanfina Ek olmalı (Fiyat + Hacim — evren daraltma)."""
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        assert m is not None, "tam_minervini block not found"
        block = m.group(1)
        n = block.count('source: "quanfina"')
        assert n == 2, (
            f"tam_minervini Quanfina Ek sayısı = {n}, beklenen 2 (Fiyat + Hacim)."
        )

    def test_ui_mark_resmi_count_is_8(self, ts):
        """tam_minervini'de tam 8 Mark Resmi Teknik olmalı (Trend Template).
        TLSMW s.79 birebir 8 madde — Hard Filter (Screen aşaması).
        """
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        assert m is not None, "tam_minervini block not found"
        block = m.group(1)
        n = block.count('source: "mark"')
        assert n == 8, (
            f"tam_minervini Mark Resmi (Trend Template) sayısı = {n}, beklenen 8 "
            f"(TLSMW s.79 birebir 8 madde)."
        )

    def test_ui_mark_ekstra_count_is_5(self, ts):
        """tam_minervini'de tam 5 Mark Ekstra olmalı (Soft Score — Recipe aşaması).
        EPS Q/Q + Sales Q/Q + ROE + Yıllık EPS + Operating Margin.
        """
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        assert m is not None, "tam_minervini block not found"
        block = m.group(1)
        n = block.count('source: "mark_ekstra"')
        assert n == 5, (
            f"tam_minervini Mark Ekstra (Soft Score) sayısı = {n}, beklenen 5 "
            f"(EPS Q/Q + Sales Q/Q + ROE + Yıllık EPS + Margin)."
        )

    def test_ui_madde_8_52w_low_is_30(self, ts):
        """tam_minervini Madde 8: 52W dipten ≥ %30 (Mark birebir, TLSMW s.79).
        KALICI İLKE #4: Önceki Trend Template UI %25 hatalıydı, %30 birebir.
        """
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        assert "dipten en az %30" in block, (
            "tam_minervini Madde 8 '52 haftalık dipten en az %30 yukarıda' eksik. "
            "Mark TLSMW s.79 birebir: '30 percent above 52-week low'."
        )

    def test_ui_has_yillik_eps(self, ts):
        """tam_minervini'de Yıllık EPS Soft Score olmalı (Gem önerisi yeni ekleme).
        TLSMW s.134-136 birebir: 'current annual growth positive, preferably breakout'.
        """
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        assert "Yıllık EPS" in block, (
            "tam_minervini'de Yıllık EPS maddesi eksik. TLSMW s.134-136 Soft Score."
        )

    def test_ui_has_operating_margin(self, ts):
        """tam_minervini'de Operating Margin expanding Soft Score olmalı.
        TLSMW s.145-147: 'expanding margins', sabit eşik yok (trend kontrolü).
        """
        pattern = r'tam_minervini:\s*\[(.*?)\]\s*,?\s*\};'
        m = re.search(pattern, ts, re.DOTALL)
        block = m.group(1)
        assert "Operating Margin" in block, (
            "tam_minervini'de Operating Margin maddesi eksik. TLSMW s.145 Soft Score."
        )

    def test_db_helpers_has_tam_minervini_slug(self, db_helpers):
        """api/db_helpers.py SCREENS_READY_9'da tam_minervini slug olmalı."""
        assert '"tam_minervini":' in db_helpers, (
            "SCREENS_READY_9 dict'inde tam_minervini slug bulunamadı."
        )

    def test_db_helpers_tam_minervini_uses_fundamental_scans_table(self, db_helpers):
        """tam_minervini slug'unun table override'ı minervini_fundamental_scans olmalı.
        Trend Template + EPS + Sales Hard Cut'tan geçmiş hisseler bu tabloda.
        """
        pattern = r'"tam_minervini":\s*\{[^}]*"table":\s*"([^"]+)"'
        m = re.search(pattern, db_helpers)
        assert m is not None, "tam_minervini entry'de table field bulunamadı"
        table = m.group(1)
        assert table == "minervini_fundamental_scans", (
            f"tam_minervini table override = '{table}', "
            f"beklenen 'minervini_fundamental_scans'."
        )

    def test_screen_categories_has_tam_minervini(self, ts):
        """SCREEN_CATEGORIES'de tam_minervini label 'Tam Minervini Tarama' olmalı."""
        pattern = r'tam_minervini:\s*"([^"]+)"'
        m = re.search(pattern, ts)
        assert m is not None, "SCREEN_CATEGORIES'de tam_minervini bulunamadı"
        assert "Tam Minervini" in m.group(1), (
            f"tam_minervini label = '{m.group(1)}', 'Tam Minervini' içermeli."
        )


class TestValidBaseConsistency:
    """valid_base (Geçerli Baz) — UI dropdown ↔ Backend SQL bire-bir uyum.

    18 Haz 2026 — P490 base kolonları (cup/flat/double quality) canlı doldu (sequence/
    NaN fix). Vizyon "geçerli baz filtresi" re-add. Backend SQL filtresi O'Neil base
    detector kolonlarını EXCELLENT/GOOD ile süzer (hard-cut, MARGINAL/NONE hariç).
    """

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_db_helpers_has_valid_base_slug(self, db_helpers):
        assert '"valid_base":' in db_helpers, (
            "SCREENS_READY_9 dict'inde valid_base slug bulunamadı."
        )

    def test_sql_filter_references_three_base_columns(self, db_helpers):
        """Filtre 3 base kalite kolonunu da içermeli (cup/flat/double)."""
        sql = _extract_sql_filter(db_helpers, "valid_base")
        for col in ("cup_handle_quality", "flat_base_quality", "double_bottom_quality"):
            assert col in sql, f"valid_base filtresinde {col} eksik. Mevcut: {sql!r}"

    def test_sql_filter_is_hard_cut_excellent_good(self, db_helpers):
        """'Geçerli' = EXCELLENT veya GOOD; MARGINAL hariç (hard-cut felsefesi)."""
        sql = _extract_sql_filter(db_helpers, "valid_base")
        assert "EXCELLENT" in sql and "GOOD" in sql, (
            f"valid_base filtresi EXCELLENT/GOOD içermeli. Mevcut: {sql!r}"
        )
        assert "MARGINAL" not in sql, (
            "valid_base filtresinde MARGINAL OLMAMALI (kusurlu — hard-cut hariç)."
        )

    def test_ui_dropdown_has_valid_base(self, ts):
        """SCREEN_CATEGORIES'de valid_base 'Geçerli Baz' label ile olmalı (dropdown)."""
        m = re.search(r'valid_base:\s*"([^"]+)"', ts)
        assert m is not None, "SCREEN_CATEGORIES'de valid_base bulunamadı (dropdown'da görünmez)."
        assert "Geçerli Baz" in m.group(1), f"valid_base label = '{m.group(1)}'."

    def test_ui_conditions_has_valid_base(self, ts):
        """SCREEN_CONDITIONS'da valid_base 2 koşullu (formasyon + kalite) olmalı."""
        n = _extract_screen_conditions_count(ts, "valid_base")
        assert n == 2, f"valid_base UI koşul sayısı = {n}, beklenen 2 (formasyon + kalite)."


class TestImportantScreensReadded:
    """18 Haz 2026 — Minervini-core önemli taramalar arşivden UI'a geri (canlı sayım
    doğrulandı: top5_rpr 38 / power_play_ready 25 / tight_low_vol_excellent 3).
    Backend slug'lar SCREENS_READY_9'da zaten kanon (KARAR #461/#466/#467); bu test
    UI<->backend bağını (dropdown + koşul + slug) korur.
    """

    SLUGS = (
        "top5_rpr", "power_play_ready", "tight_low_vol_excellent",
        "new_top5_rpr", "healthy_accumulation", "rpr_89_tpr_c",
        # 18 Haz 2026 — yüksek-conviction izleme (bugün 0 ama kolon DOLU, canon eşik):
        "vcp_ready_high", "tennis_ball_active",
    )

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_backend_slugs_exist(self, db_helpers):
        for slug in self.SLUGS:
            assert f'"{slug}":' in db_helpers, f"SCREENS_READY_9'da {slug} yok (backend)."

    def test_ui_dropdown_has_all(self, ts):
        for slug in self.SLUGS:
            assert re.search(rf'{slug}:\s*"[^"]+"', ts), (
                f"SCREEN_CATEGORIES'de {slug} yok (dropdown'da görünmez)."
            )

    def test_ui_conditions_present(self, ts):
        for slug in self.SLUGS:
            n = _extract_screen_conditions_count(ts, slug)
            assert n >= 1, f"SCREEN_CONDITIONS'da {slug} koşulu yok (n={n})."


class TestMeanReversionScreen:
    """P502 — Carr Mean Reversion bulk screen (pvh-hesap, scanner/deploy yok).
    UI dropdown + koşul (carr source) + backend dispatch/fonksiyon bağı korunur.
    """

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_backend_function_and_dispatch(self, db_helpers):
        assert "def screen_mean_reversion_get_results" in db_helpers, "MR screen fonksiyonu yok."
        assert 'if slug == "mean_reversion"' in db_helpers, "dispatch MR branch yok."

    def test_carr_prefilter_present(self, db_helpers):
        """Carr s.302 on-filtre (Bullish Watch List) — falling-knife/value-trap onleme.
        Cift danisma (18 Haz 2026) bulgu #2: MR on-filtresiz tehlikeli.
        """
        m = re.search(r"def screen_mean_reversion_get_results.*?\n(?=def )", db_helpers, re.DOTALL)
        assert m, "MR fonksiyonu bulunamadi."
        body = m.group(0)
        assert "last_close <= 5.0" in body, "Carr s.302 Fiyat>$5 on-filtresi yok."
        assert "avg_vol < 100_000" in body, "Carr s.302 hacim>=100k on-filtresi yok."
        assert "s.302" in body, "Carr s.302 atfi yok (Kural #26 kaynak seffaflik)."

    def test_ui_dropdown_has_mean_reversion(self, ts):
        assert re.search(r'mean_reversion:\s*"[^"]+"', ts), "SCREEN_CATEGORIES'de mean_reversion yok."

    def test_ui_conditions_use_carr_source(self, ts):
        """Carr stratejisi — kaynak 'carr' (Mark değil, doğru atıf)."""
        n = _extract_screen_conditions_count(ts, "mean_reversion")
        assert n >= 1, f"mean_reversion koşulu yok (n={n})."
        m = re.search(r'mean_reversion:\s*\[(.*?)\]\s*,', ts, re.DOTALL)
        assert m and 'source: "carr"' in m.group(1), "mean_reversion koşulları 'carr' source kullanmalı."

    def test_carr_source_registered(self, ts):
        """ConditionSource + CONDITION_SOURCE_LABEL 'carr' tanımlı olmalı."""
        assert '"carr"' in ts
        assert re.search(r'carr:\s*"[^"]+"', ts), "CONDITION_SOURCE_LABEL.carr yok."


class TestCoiledSpringScreen:
    """P511 — Carr Coiled Spring bulk screen (pvh 80>=60, MR pateni).
    TIER-2 eyeball ADAY; UI dropdown + koşul (carr source) + backend dispatch/fonksiyon.
    """

    @pytest.fixture(scope="class")
    def ts(self) -> str:
        return _read("web/types/screens.ts")

    @pytest.fixture(scope="class")
    def db_helpers(self) -> str:
        return _read("api/db_helpers.py")

    def test_backend_function_and_dispatch(self, db_helpers):
        assert "def screen_coiled_spring_get_results" in db_helpers, "Coiled Spring fonksiyonu yok."
        assert 'if slug == "coiled_spring"' in db_helpers, "dispatch coiled_spring branch yok."

    def test_60_bar_requirement(self, db_helpers):
        """Coiled Spring 60 bar gerektirir (pvh 80 yeterli — Pullback/BlueSky'in aksine)."""
        m = re.search(r"def screen_coiled_spring_get_results.*?\n(?=def )", db_helpers, re.DOTALL)
        assert m, "Coiled Spring fonksiyonu bulunamadi."
        body = m.group(0)
        assert "len(arr) < 60" in body, "60 bar esigi yok."
        assert "compute_coiled_spring" in body, "compute_coiled_spring cagrisi yok."

    def test_carr_prefilter_present(self, db_helpers):
        """Carr s.244 on-filtre (MR s.302 pateni) — Fiyat>$5 + hacim>=100k."""
        m = re.search(r"def screen_coiled_spring_get_results.*?\n(?=def )", db_helpers, re.DOTALL)
        body = m.group(0)
        assert "last_close <= 5.0" in body, "Fiyat>$5 on-filtresi yok."
        assert "avg_vol < 100_000" in body, "hacim>=100k on-filtresi yok."

    def test_ui_dropdown_and_carr_conditions(self, ts):
        assert re.search(r'coiled_spring:\s*"[^"]+"', ts), "SCREEN_CATEGORIES'de coiled_spring yok."
        n = _extract_screen_conditions_count(ts, "coiled_spring")
        assert n >= 1, f"coiled_spring koşulu yok (n={n})."
        m = re.search(r'coiled_spring:\s*\[(.*?)\]\s*,', ts, re.DOTALL)
        assert m and 'source: "carr"' in m.group(1), "coiled_spring koşulları 'carr' source kullanmalı."

    def test_ui_mentions_eyeball(self, ts):
        """TIER-2 dürüstlük: UI koşulunda EYEBALL/göz kararı uyarısı olmalı (Kural #26)."""
        m = re.search(r'coiled_spring:\s*\[(.*?)\]\s*,', ts, re.DOTALL)
        block = m.group(1).upper()
        assert "EYEBALL" in block or "GÖZ KARAR" in block, "TIER-2 eyeball uyarisi yok."
