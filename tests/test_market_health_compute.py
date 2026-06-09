"""
_compute_market_health — Paket 382 (AÇIK KONU #58 çözümü, clean-room MOCK→gerçek).

Çift Danışma kanıt zinciri:
- Mark kitap kanon DD>=4 -> "Under Pressure" (TLSMW Bol. 5)
- Mark kitap kanon Probability Convergence (TLSMW Bol. 10)
- Markets360 Bundle GO/CAUTION/NO-GO + "YESIL/SARI/KIRMIZI" sizma riski -> KULLANILMADI
- Skor degerleri 25/50/75 Quanfina ic tasarim karari (UI 0-100 zorunlu)
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


_h = api_main._compute_market_health


class TestDistributionDayOverride:
    """Mark kitap kanon TLSMW Bol. 5: DD>=4 -> 'Under Pressure' kategorik override.

    Stage'den bagimsiz — DD baskini onceliklidir.
    """

    def test_dd_4_under_pressure(self):
        # 3/3 Stage 2 (advancing) bile DD>=4 ise -> UNDER_PRESSURE
        assert _h(4, 2, 2, 2) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_dd_5_under_pressure(self):
        assert _h(5, 2, 2, 2) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_dd_10_under_pressure(self):
        # Cok yuksek DD -> hala UNDER_PRESSURE (kategori sabit)
        assert _h(10, 2, 2, 2) == (25, "UNDER_PRESSURE", "DEFENSIVE")


class TestFtdSymmetry:
    """P424 (31 May 2026 — derin tarama F3): Follow-Through Day simetrisi.

    IBD Market Pulse SIMETRIK — DD birikimi zayiflik, hacim-onayli FTD yeni
    uptrend onayi. Hacim-onayli FTD, DD baskisini UNDER_PRESSURE -> NEUTRAL
    yumusatir. AMA 2+ Stage 4 yapisal bear'a DOKUNMAZ.
    """

    def test_dd4_with_ftd_softens_to_neutral(self):
        # DD>=4 ama hacim-onayli FTD -> NEUTRAL (toparlanma), DEFENSIVE degil
        assert _h(4, 2, 2, 2, ftd_confirmed=True) == (50, "NEUTRAL", "CAUTION")

    def test_dd4_without_ftd_stays_under_pressure(self):
        # FTD yok -> eski davranis (UNDER_PRESSURE) korunur
        assert _h(4, 2, 2, 2, ftd_confirmed=False) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_dd_default_no_ftd_backward_compat(self):
        # ftd_confirmed default False -> eski 4-arg cagrilarla geriye uyum
        assert _h(5, 2, 2, 2) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_ftd_does_not_rescue_structural_bear(self):
        # 2+ Stage 4 (yapisal bear) + FTD bile -> UNDER_PRESSURE kalir
        # (DD<4 oldugu icin DD-yumusatma devreye girmez; Stage 4 baskin)
        assert _h(0, 4, 4, 2, ftd_confirmed=True) == (25, "UNDER_PRESSURE", "DEFENSIVE")
        assert _h(0, 4, 4, 4, ftd_confirmed=True) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_ftd_irrelevant_when_healthy(self):
        # DD<4 + 3/3 Stage 2 zaten HEALTHY — FTD durumu degistirmez
        assert _h(0, 2, 2, 2, ftd_confirmed=True) == (75, "HEALTHY", "LONG")

    def test_dd4_and_2stage4_with_ftd_still_pressure(self):
        # Hem DD>=4 hem 2+ Stage 4 + FTD: P424 reorder sonrasi YAPISAL BEAR dali
        # (2+ Stage 4) ONCE gelir -> UNDER_PRESSURE. FTD yapisal bear'i KURTARMAZ.
        # Bu, docstring niyetiyle tutarli (FTD sadece saf DD baskisini yumusatir).
        assert _h(4, 4, 4, 2, ftd_confirmed=True) == (25, "UNDER_PRESSURE", "DEFENSIVE")


class TestProbabilityConvergence:
    """Mark kitap kanon TLSMW Bol. 10: 3 endeks hizalanmasi (Stage 2)."""

    def test_3_of_3_stage2_healthy(self):
        # Tum endeks Stage 2 + DD<4 -> HEALTHY (full convergence)
        assert _h(0, 2, 2, 2) == (75, "HEALTHY", "LONG")

    def test_3_of_3_stage2_dd3_still_healthy(self):
        # DD=3 hala <4 esiginde -> HEALTHY (override degil)
        assert _h(3, 2, 2, 2) == (75, "HEALTHY", "LONG")

    def test_2_of_3_stage2_neutral(self):
        # Karisik: 2 Stage 2 + 1 baska, Stage 4 yok -> NEUTRAL (selective)
        assert _h(0, 2, 2, 1) == (50, "NEUTRAL", "CAUTION")
        assert _h(0, 2, 2, 3) == (50, "NEUTRAL", "CAUTION")

    def test_1_of_3_stage2_neutral(self):
        # 1 Stage 2 + diger Stage 1/3 (Stage 4 yok) -> NEUTRAL
        assert _h(0, 2, 1, 3) == (50, "NEUTRAL", "CAUTION")


class TestBearOverride:
    """2+ endeks Stage 4 (declining) -> UNDER_PRESSURE (bear confirm)."""

    def test_2_of_3_stage4_under_pressure(self):
        assert _h(0, 4, 4, 2) == (25, "UNDER_PRESSURE", "DEFENSIVE")
        assert _h(0, 4, 4, 1) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_3_of_3_stage4_under_pressure(self):
        # Tam bear -> UNDER_PRESSURE
        assert _h(0, 4, 4, 4) == (25, "UNDER_PRESSURE", "DEFENSIVE")


class TestEdgeAndFallback:
    """Hicbir Stage 2 yok, Stage 4 az -> guvenli UNDER_PRESSURE fallback."""

    def test_no_stage2_no_stage4_under_pressure(self):
        # Hepsi Stage 1 (basing) — alim sinyali yok
        assert _h(0, 1, 1, 1) == (25, "UNDER_PRESSURE", "DEFENSIVE")

    def test_no_stage2_with_single_stage4(self):
        # 1 Stage 4 + diger Stage 1/3 -> UNDER_PRESSURE
        assert _h(0, 4, 1, 3) == (25, "UNDER_PRESSURE", "DEFENSIVE")


class TestReturnShape:
    """Donus tipi disiplini: (int, Literal, Literal) tuple."""

    def test_score_in_0_100_range(self):
        for dd in (0, 1, 2, 3, 4, 5):
            for stages in [(2, 2, 2), (4, 4, 4), (1, 2, 3), (3, 1, 4)]:
                score, label, mode = _h(dd, *stages)
                assert 0 <= score <= 100
                assert label in {"HEALTHY", "NEUTRAL", "UNDER_PRESSURE"}
                assert mode in {"LONG", "CAUTION", "DEFENSIVE"}

    def test_label_clean_room_no_legacy(self):
        # Sizma kontrol regresyonu: "YESIL/SARI/KIRMIZI" donmemesi (P382 clean-room)
        legacy = {"YEŞİL", "SARI", "KIRMIZI", "YESIL"}
        for dd in (0, 2, 4):
            _, label, _ = _h(dd, 2, 2, 2)
            assert label not in legacy
