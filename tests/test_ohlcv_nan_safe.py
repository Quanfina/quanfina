"""
OHLCV NaN-safe regression (Paket 351) — yfinance NaN satirlari atlanmali.

Bug sinifi (Paket 348/350 ile ayni JSON-serialize ailesi): _fetch_ohlcv_real
yfinance hist'inden bar olustururken float(row["Close"]) NaN'i raise ETMEZ
(int(Volume) raise eder ama OHLC degil). NaN close bar:
  1. /api/stock/{symbol}/ohlcv response (list[OhlcvBar]) JSON serialize -> 500
  2. downstream compute_* helper'lara NaN zehirlenmesi (RS/Stage/ATR vb.)

yfinance settle olmamis gun / split-gap / partial day icin NaN satir dondurur
(paper trading canli veri kaynagi). Fix: NaN satirlari atla (scanner.py dropna
disiplini ile ayni). Bu test mock yfinance ile NaN satir enjekte eder.

Kaynak: Kural #24 (Saglam Gidelim — Asama 5 PYTEST), AÇIK KONU #75 (yfinance pipeline).
"""
import sys
import time
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
    import pandas as pd
    import numpy as np
except ImportError:
    pytest.skip("fastapi/pandas yok", allow_module_level=True)


def _make_hist(n: int = 15, nan_col: str | None = None, nan_row: int = 5) -> "pd.DataFrame":
    """yfinance history() benzeri DataFrame — istege bagli bir hucre NaN."""
    idx = pd.date_range("2025-01-02", periods=n, freq="B")  # is gunleri
    df = pd.DataFrame(
        {
            "Open":   [100.0 + i for i in range(n)],
            "High":   [101.0 + i for i in range(n)],
            "Low":    [ 99.0 + i for i in range(n)],
            "Close":  [100.5 + i for i in range(n)],
            "Volume": [1_000_000 + i * 1000 for i in range(n)],
        },
        index=idx,
    )
    if nan_col is not None:
        df.iloc[nan_row, df.columns.get_loc(nan_col)] = np.nan
    return df


class _FakeTicker:
    """yfinance.Ticker stub — sabit hist dondurur."""
    _hist: "pd.DataFrame" = _make_hist()

    def __init__(self, symbol):
        self.symbol = symbol

    def history(self, period="1y"):
        return _FakeTicker._hist


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    api_main._OHLCV_CACHE.clear()
    api_main._YF_DISABLED = False
    monkeypatch.setattr("yfinance.Ticker", _FakeTicker)
    yield
    api_main._OHLCV_CACHE.clear()


def _set_hist(df):
    _FakeTicker._hist = df


def _assert_no_nan(bars):
    for b in bars:
        for field in (b.open, b.high, b.low, b.close):
            assert field == field, f"NaN OHLC kaldi: {b}"   # NaN != NaN
        assert isinstance(b.volume, int)


class TestOhlcvNanSafe:

    def test_clean_frame_all_bars_returned(self):
        _set_hist(_make_hist(15))
        bars = api_main._fetch_ohlcv_real("CLEANSYM", 252)
        assert bars is not None
        assert len(bars) == 15
        _assert_no_nan(bars)

    def test_nan_close_row_skipped(self):
        _set_hist(_make_hist(15, nan_col="Close", nan_row=5))
        bars = api_main._fetch_ohlcv_real("NANCLOSE", 252)
        assert bars is not None
        assert len(bars) == 14   # 1 NaN satir atlandi
        _assert_no_nan(bars)

    def test_nan_open_row_skipped(self):
        _set_hist(_make_hist(15, nan_col="Open", nan_row=3))
        bars = api_main._fetch_ohlcv_real("NANOPEN", 252)
        assert bars is not None
        assert len(bars) == 14
        _assert_no_nan(bars)

    def test_nan_volume_row_skipped_not_crash(self):
        # Eski kod: int(NaN) -> ValueError -> tum sembol MOCK'a duserdi.
        # Yeni kod: sadece o satir atlanir, geri kalan gercek veri korunur.
        _set_hist(_make_hist(15, nan_col="Volume", nan_row=7))
        bars = api_main._fetch_ohlcv_real("NANVOL", 252)
        assert bars is not None
        assert len(bars) == 14
        _assert_no_nan(bars)

    def test_serialized_bars_json_safe(self):
        import json
        _set_hist(_make_hist(15, nan_col="Close", nan_row=2))
        bars = api_main._fetch_ohlcv_real("JSONSAFE", 252)
        payload = [
            {"time": b.time, "open": b.open, "high": b.high,
             "low": b.low, "close": b.close, "volume": b.volume}
            for b in bars
        ]
        # FastAPI encoder allow_nan=False — NaN olsa ValueError firlatirdi
        json.dumps(payload, allow_nan=False)
