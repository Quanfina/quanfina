"""P466 /signals stop/target/R-R turetme matematigi — DB-bagimsiz unit test (Paket 468).

Neden (review test-gap bulgusu): _build_signal (api/main.py) canli stop/hedef/R-R
hesaplar: stop = compute_atr_volatility(...).suggested_stop_normal (Entry - 2.5xATR),
target = round(entry + 3.0*(entry - stop), 2) (3R hedef), risk_reward = _calc_rr(...).
Yapica R-R her sinyal icin ~3.0 olmali. _calc_rr saf bir fonksiyon ama HIC test
edilmemis — 3.0 carpanini bozan, entry/stop sirasini ters ceviren veya risk<=0
guard'ini kiran bir regresyon sessizce gecerdi. Bu dosya saf matematigi kilitler.

api/.venv altinda calisir (main import -> fastapi). Root venv'de fastapi yok -> skip.
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
    pytest.skip("fastapi yok (api/.venv gerekli)", allow_module_level=True)

_calc_rr = api_main._calc_rr


# --- Part 1: saf _calc_rr birim testleri ---

def test_calc_rr_basic_3r():
    # risk = 100-95 = 5, reward = 115-100 = 15 -> 15/5 = 3.0
    assert _calc_rr(100, 95, 115) == 3.0


def test_calc_rr_none_stop():
    assert _calc_rr(100, None, 115) is None


def test_calc_rr_none_target():
    assert _calc_rr(100, 95, None) is None


def test_calc_rr_stop_above_price_risk_nonpositive():
    # stop fiyatin USTUNDE -> risk = price-stop <= 0 -> None
    assert _calc_rr(100, 105, 115) is None


def test_calc_rr_target_below_price_reward_nonpositive():
    # target fiyatin ALTINDA -> reward = target-price <= 0 -> None
    assert _calc_rr(100, 95, 90) is None


# --- Part 2: P466 3R kontrat invariantini kilitle ---

@pytest.mark.parametrize("entry,stop", [
    (100.0, 95.0),
    (212.50, 207.0),
    (145.20, 141.50),
    (1450.30, 1420.0),
    (37.80, 36.10),
])
def test_3r_target_yields_rr_three(entry, stop):
    # P466 turetmesi: target = entry + 3*(entry-stop) -> R-R == 3.0 (yuvarlamayla ~3.0)
    assert stop < entry
    target = round(entry + 3.0 * (entry - stop), 2)
    rr = _calc_rr(entry, stop, target)
    assert rr == pytest.approx(3.0, abs=0.05), (
        f"entry={entry} stop={stop} target={target} -> R-R={rr} (3.0 bekleniyor)"
    )
