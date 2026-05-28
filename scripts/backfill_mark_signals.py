"""Mark canon kolon backfill — Migration 004-009 sonrasi DB doldurma (A1).

minervini_scans son scan_date satirlarinda Mark kolonlari NULL (22 May taramasi
pvh yazmamis). yfinance ile taze OHLCV cekip scanner.py DRY helper pattern ile
Mark kolonlarini hesaplar + UPDATE.

Kullanim:
    python scripts/backfill_mark_signals.py            # ilk 25 passed=1 sembol
    python scripts/backfill_mark_signals.py --limit 50 # ilk 50
    python scripts/backfill_mark_signals.py --symbols NVDA,AAPL,MSFT

Idempotent: UPDATE (mevcut satir gunceller). Tekrar guvenli.
DRY: scanner.py save_results Mark helper cagri pattern birebir.
"""
import os
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yfinance as yf  # noqa: E402
import psycopg2  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from quanfina_math import (  # noqa: E402
    compute_vcp_quality,
    compute_vcp_ready_score,
    compute_power_play_pass,
    detect_tennis_ball,
    compute_volume_asymmetry,
    compute_carr_stage,
)

load_dotenv()


def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=30,
    )


def build_pvh(df):
    """scanner.py DRY: son 80 gun OHLCV -> pvh dict listesi."""
    tail = df.tail(80)
    pvh = []
    for idx, row in tail.iterrows():
        try:
            pvh.append({
                "date": str(idx.date()),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            })
        except Exception:
            continue
    return pvh


def compute_mark_fields(df):
    """yfinance OHLCV -> Mark canon dict (scanner.py save_results DRY)."""
    pvh = build_pvh(df)
    closes = [float(x) for x in df["Close"].dropna().tolist()]
    volumes = [float(x) for x in df["Volume"].dropna().tolist()]

    out = {}
    # VCP kalite + ready + power play (pvh bazli)
    out["vcp_quality_score"] = compute_vcp_quality(pvh)
    out["vcp_ready_score"] = compute_vcp_ready_score(pvh)
    out["power_play_pass"] = compute_power_play_pass(pvh)

    # Tennis ball (breakout_idx = son 20 gun en yuksek close)
    tb_pattern = None
    if pvh and len(pvh) >= 20:
        try:
            window = min(20, len(pvh))
            seg = pvh[-window:]
            local_idx = max(range(window), key=lambda i: seg[i].get("close", 0))
            breakout_idx = len(pvh) - window + local_idx
            tb = detect_tennis_ball(breakout_idx, pvh)
            tb_pattern = tb.get("pattern")
        except Exception:
            pass
    out["tennis_ball_pattern"] = tb_pattern

    # Volume asymmetry
    va_ratio = None
    va_tier = None
    if pvh and len(pvh) >= 5:
        try:
            va = compute_volume_asymmetry(pvh, lookback_days=20)
            va_ratio = va.get("asymmetry_ratio")
            va_tier = va.get("tier")
        except Exception:
            pass
    out["volume_asymmetry_ratio"] = va_ratio
    out["volume_asymmetry_tier"] = va_tier

    # Carr Stage (closes/volumes full series, 150 gun MA)
    cs_stage = None
    cs_label = None
    cs_slope = None
    cs_ma = None
    cs_pvm = None
    if len(closes) >= 150 and len(closes) == len(volumes):
        try:
            cs = compute_carr_stage(closes, volumes)
            cs_stage = cs.get("stage")
            cs_label = cs.get("stage_label")
            cs_slope = cs.get("slope_pct_per_year")
            cs_ma = cs.get("ma_value")
            cs_pvm = cs.get("price_vs_ma_pct")
        except Exception:
            pass
    out["carr_stage"] = cs_stage
    out["carr_stage_label"] = cs_label
    out["carr_slope_pct_per_year"] = cs_slope
    out["carr_ma_value"] = cs_ma
    out["carr_price_vs_ma_pct"] = cs_pvm
    return out


def main():
    args = sys.argv[1:]
    limit = 25
    symbols_override = None
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])
    if "--symbols" in args:
        symbols_override = args[args.index("--symbols") + 1].split(",")

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT MAX(scan_date) FROM minervini_scans")
    last = cur.fetchone()[0]
    print(f"Son scan_date: {last}")

    if symbols_override:
        symbols = [s.strip().upper() for s in symbols_override]
    else:
        cur.execute(
            "SELECT ticker FROM minervini_scans "
            "WHERE scan_date = %s AND passed = 1 "
            "ORDER BY ticker LIMIT %s",
            (last, limit),
        )
        symbols = [r[0] for r in cur.fetchall()]
    print(f"{len(symbols)} sembol backfill: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

    updated = 0
    carr_filled = 0
    for sym in symbols:
        try:
            df = yf.download(sym, period="1y", progress=False, auto_adjust=True)
            if df is None or df.empty or len(df) < 60:
                print(f"  [SKIP] {sym}: yetersiz veri")
                continue
            # yfinance multi-index kolon duzlestme (tek sembol)
            if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
                df.columns = df.columns.get_level_values(0)
            fields = compute_mark_fields(df)
            cur.execute(
                """UPDATE minervini_scans SET
                    vcp_quality_score = %s,
                    vcp_ready_score = %s,
                    power_play_pass = %s,
                    tennis_ball_pattern = %s,
                    volume_asymmetry_ratio = %s,
                    volume_asymmetry_tier = %s,
                    carr_stage = %s,
                    carr_stage_label = %s,
                    carr_slope_pct_per_year = %s,
                    carr_ma_value = %s,
                    carr_price_vs_ma_pct = %s
                   WHERE ticker = %s AND scan_date = %s""",
                (
                    fields["vcp_quality_score"], fields["vcp_ready_score"],
                    fields["power_play_pass"], fields["tennis_ball_pattern"],
                    fields["volume_asymmetry_ratio"], fields["volume_asymmetry_tier"],
                    fields["carr_stage"], fields["carr_stage_label"],
                    fields["carr_slope_pct_per_year"], fields["carr_ma_value"],
                    fields["carr_price_vs_ma_pct"],
                    sym, last,
                ),
            )
            if cur.rowcount > 0:
                updated += 1
                if fields["carr_stage"] is not None:
                    carr_filled += 1
                cs = fields["carr_stage"]
                vcp = fields["vcp_quality_score"]
                print(f"  [OK] {sym}: carr_stage={cs}, vcp={vcp}, pp={fields['power_play_pass']}")
        except Exception as e:
            print(f"  [HATA] {sym}: {str(e)[:80]}")
            continue

    conn.commit()
    conn.close()
    print(f"\n[TAMAM] {updated} satir UPDATE, {carr_filled} carr_stage dolu.")


if __name__ == "__main__":
    main()
