"""
Tek seferlik seed — web_watchlist + web_trades boşsa eski mock veriyi yükler.
Idempotent: tablolar doluysa hiçbir şey yapmaz.
"""
from __future__ import annotations
import sys
from decimal import Decimal as _D, ROUND_HALF_UP as _RHU
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "api"))
from db_helpers import (
    watchlist_get_all, watchlist_insert, watchlist_recompute_consensus,
    trades_get_all, trades_insert,
)


# ── Watchlist seed verisi (eski MOCK_WATCHLIST tam kopyası) ──────────────────
# symbol, strategy, status, price, added_date, setup_type, pivot_price, note, rs_rating
_WATCHLIST_RAW = [
    ("NVDA",  "minervini", "buy",      875.40, "2026-05-16", "VCP",                   820.00, None,            97),
    ("AVGO",  "minervini", "buy",     1680.20, "2026-05-12", None,                   1620.00, None,            94),
    ("META",  "minervini", "focus",    525.80, "2026-05-10", None,                    505.00, None,            92),
    ("MSFT",  "minervini", "focus",    415.60, "2026-05-10", None,                      None, None,            88),
    ("COST",  "minervini", "focus",    890.30, "2026-05-09", None,                      None, None,            85),
    ("GOOGL", "minervini", "on_deck",  178.40, "2026-05-08", None,                      None, None,            83),
    ("ASML",  "minervini", "on_deck",  742.10, "2026-05-08", None,                      None, None,            81),
    ("AMZN",  "minervini", "on_deck",  196.70, "2026-05-07", None,                      None, None,            80),
    ("ORCL",  "minervini", "on_deck",  148.90, "2026-05-05", None,                      None, None,            76),
    ("CRM",   "minervini", "on_deck",  296.40, "2026-05-05", None,                      None, None,            74),
    ("NFLX",  "minervini", "watch",    672.30, "2026-05-03", None,                      None, None,            79),
    ("AMD",   "minervini", "watch",    158.70, "2026-05-03", None,                      None, None,            73),
    ("ADBE",  "minervini", "watch",    394.50, "2026-05-02", None,                      None, None,            71),
    ("V",     "minervini", "watch",    275.80, "2026-05-01", None,                      None, None,            75),
    ("MA",    "minervini", "watch",    477.20, "2026-05-01", None,                      None, None,            73),
    ("JPM",   "minervini", "watch",    215.40, "2026-04-28", None,                      None, None,            70),
    ("PLTR",  "minervini", "watch",     22.80, "2026-04-25", None,                      None, None,            65),
    ("COIN",  "minervini", "watch",    152.40, "2026-04-24", None,                      None, None,            68),
    ("DASH",  "minervini", "watch",    118.60, "2026-04-22", None,                      None, None,            63),
    ("SHOP",  "minervini", "watch",     74.20, "2026-04-18", None,                      None, None,            61),
    ("NVDA",  "carr",      "focus",    875.40, "2026-05-13", "Pullback",              820.00, "MA50 destek",   97),
    ("META",  "carr",      "focus",    525.80, "2026-05-10", "Pullback",              505.00, None,            92),
    ("GOOGL", "carr",      "on_deck",  178.40, "2026-05-08", "Coiled Spring",           None, None,            83),
    ("AMZN",  "carr",      "on_deck",  196.70, "2026-05-07", "Coiled Spring",           None, None,            80),
    ("MSFT",  "carr",      "on_deck",  415.60, "2026-05-10", "Bullish Divergence",      None, None,            88),
    ("AAPL",  "carr",      "watch",    182.30, "2026-05-03", "Bullish Divergence",      None, None,            72),
    ("TSM",   "carr",      "watch",    142.80, "2026-05-02", "Blue Sky Breakout",       None, None,            78),
    ("AMD",   "carr",      "watch",    158.70, "2026-05-03", "Blue Sky Breakout",       None, None,            73),
    ("LLY",   "carr",      "watch",    724.50, "2026-04-28", "Bullish Base Breakout",   None, None,            82),
    ("UBER",  "carr",      "watch",     68.40, "2026-04-25", "Bullish Base Breakout",   None, None,            70),
]


def _calc_pl(entry: float, exit_: float, shares: int):
    ep = _D(str(entry)); xp = _D(str(exit_)); qty = _D(str(shares))
    pld = float(((xp - ep) * qty).quantize(_D("0.01"), rounding=_RHU))
    plp = float(((xp - ep) / ep * 100).quantize(_D("0.01"), rounding=_RHU))
    return pld, plp


# ── Trades seed verisi (eski MOCK_TRADES tam kopyası) ────────────────────────
def _closed(sym, strat, setup, ed, ep, sh, xd, xp, grade, reason, lessons=None):
    pld, plp = _calc_pl(ep, xp, sh)
    return {
        "symbol": sym, "strategy": strat, "setup_type": setup,
        "entry_date": ed, "entry_price": ep,
        "exit_date": xd, "exit_price": xp,
        "shares": sh, "status": "closed",
        "pl_dollar": pld, "pl_pct": plp,
        "grade": grade, "exit_reason": reason, "lessons": lessons,
    }

def _open(sym, strat, setup, ed, ep, sh):
    return {
        "symbol": sym, "strategy": strat, "setup_type": setup,
        "entry_date": ed, "entry_price": ep,
        "exit_date": None, "exit_price": None,
        "shares": sh, "status": "open",
        "pl_dollar": None, "pl_pct": None,
        "grade": None, "exit_reason": None, "lessons": None,
    }


_TRADES_SEED = [
    _closed("NVDA",  "minervini", "vcp",           "2025-12-01", 700.00,  50, "2026-02-15", 826.00,  "A+", "target_hit",    "Mükemmel kırılım, plan tam uygulandı. Hacim onayı çok güçlüydü."),
    _closed("AVGO",  "minervini", "pivot",          "2026-01-10", 1480.00, 20, "2026-03-05", 1657.60, "A",  "target_hit",    "Giriş temiz, trailing stop ile çıkış erken olabilirdi."),
    _closed("META",  "carr",      "pullback",       "2026-01-20", 496.00,  30, "2026-03-15", 525.76,  "B",  "trailing_stop", None),
    _closed("MSFT",  "minervini", "pocket_pivot",   "2026-01-15", 392.00,  15, "2026-04-01", 423.36,  "B",  "trailing_stop", None),
    _closed("AAPL",  "carr",      "coiled_spring",  "2026-02-01", 188.00,  40, "2026-03-10", 184.24,  "A",  "stop_loss",     "Stop zamanında uygulandı. Giriş doğruydu, piyasa döndü. Grade A çünkü kurala uyuldu."),
    _closed("TSM",   "carr",      "pullback",       "2026-02-10", 160.00,  25, "2026-03-20", 152.00,  "C",  "stop_loss",     "Stop geç uygulandı. Daha çabuk kesilmeli. Kurala uymama nedeniyle C."),
    _closed("AMD",   "minervini", "vcp",            "2026-03-01", 158.00,  30, "2026-04-15", 158.00,  "B",  "discretionary", "Yeterli momentum gelmedi, tasfiye edildi. Break-even kabul."),
    _open("GOOGL",   "minervini", "vcp",            "2026-04-10", 165.00,  35),
    _open("AMZN",    "carr",      "coiled_spring",  "2026-05-01", 185.00,  25),
    _open("LLY",     "minervini", "pivot",          "2026-05-10", 710.00,  10),
]


def main():
    # Watchlist
    if watchlist_get_all():
        print("web_watchlist zaten dolu — watchlist seed atlandı")
    else:
        for r in _WATCHLIST_RAW:
            watchlist_insert({
                "symbol":      r[0], "strategy":    r[1], "status":  r[2],
                "price":       r[3], "added_date":  r[4], "setup_type": r[5],
                "pivot_price": r[6], "note":        r[7], "rs_rating":  r[8],
                "consensus_count": 1, "consensus_strategies": [r[1]],
            })
        watchlist_recompute_consensus()
        print(f"✅ {len(_WATCHLIST_RAW)} watchlist satırı yüklendi")

    # Trades
    if trades_get_all():
        print("web_trades zaten dolu — trades seed atlandı")
    else:
        for t in _TRADES_SEED:
            trades_insert(t)
        print(f"✅ {len(_TRADES_SEED)} trade yüklendi")


if __name__ == "__main__":
    main()
