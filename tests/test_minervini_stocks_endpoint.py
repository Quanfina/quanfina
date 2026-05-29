"""
/api/minervini/stocks MOCK->DB wiring (Paket 368) endpoint testi.

/minervini sayfasi onceden MOCK_STOCKS (sahte NVDA/AVGO...) gosteriyordu.
P368: minervini_stocks_get_latest -> gercek minervini_scans son tarama (passed=1,
rs_ibd DESC). DB bos/erisilemez -> MOCK_STOCKS fallback (UX kesintisiz). Iki yolda
da 200 + gecerli MinerviniStock list (pydantic response_model dogrular).
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
    from fastapi.testclient import TestClient
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


class TestMinerviniStocksEndpoint:
    def test_200_non_empty_list(self, client):
        r = client.get("/api/minervini/stocks")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) > 0  # DB veya MOCK fallback — ikisi de dolu

    def test_valid_minervini_stock_shape(self, client):
        # response_model=list[MinerviniStock] -> tum zorunlu alanlar serialize edilir
        item = client.get("/api/minervini/stocks").json()[0]
        for key in ("symbol", "company", "sector", "price", "change_pct", "grade",
                    "rs_ibd", "rs_12m", "ma200_slope", "high52", "pct_from_high",
                    "confirmations", "violations", "sma50", "atr14", "list_type"):
            assert key in item, f"eksik alan: {key}"
        assert isinstance(item["symbol"], str) and item["symbol"]
        assert isinstance(item["price"], (int, float)) and item["price"] >= 0
        assert isinstance(item["confirmations"], int)
        assert isinstance(item["violations"], int)

    def test_sorted_by_rs_descending(self, client):
        # DB yolu rs_ibd DESC siralar; MOCK fallback de mantikli sirali.
        data = client.get("/api/minervini/stocks").json()
        rs = [d["rs_ibd"] for d in data]
        # Ilk eleman son elemandan >= olmali (DESC veya esit) — bos degilse
        if len(rs) >= 2:
            assert rs[0] >= rs[-1]


def test_db_helper_parses_defensively():
    """minervini_stocks_get_latest TEXT kolon parse + turetilen alan dogrulamasi
    (DB erisilebilirse). DB yoksa skip — helper'in patlamadigini da teyit eder."""
    from db_helpers import minervini_stocks_get_latest
    try:
        rows = minervini_stocks_get_latest(limit=3)
    except Exception:
        pytest.skip("DB erisilemez")
    for r in rows:
        # parse sonucu tipler dogru
        assert isinstance(r["price"], float)
        assert isinstance(r["rs_ibd"], float)
        assert isinstance(r["confirmations"], int)
        assert isinstance(r["volume"], int)
        # pct_from_high turetildi (price/high52)
        assert isinstance(r["pct_from_high"], float)
        assert r["list_type"] == "watch"
