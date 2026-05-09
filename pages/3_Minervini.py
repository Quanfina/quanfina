"""
3_Minervini.py - Mark Minervini SEPA Stratejisi (Yeni 4 Liste Mimarisi)

ADIM 4 Sprint 4.2: 4 dis tab iskelet
- Watch (100+ hisse)    - Genel bakis
- On Deck (30-40)       - On-eleme
- Focus (5-15)          - Detayli analiz
- Buy (3-5)             - Alim hazirligi

Her tab symbol_lists tablosundan oku (list_type kolonu ile filtreli).
Henuz veri yok - sonraki sprintlerde populate edilecek.

Eski 7+4 tab yapisi: pages/3_Minervini_old.py (rollback icin).
"""
import json
from datetime import date, datetime, timedelta
import streamlit as st
import pandas as pd
from db_connection import get_connection, add_to_list, remove_from_list, get_setup_types, promote_to_list
from _list_cols import get_columns_for_list, get_label, get_list_meta, LIST_TYPES
from quanfina_math import (  # Sprint 4.7e.2 + 4.7e.3
    check_50ma_trail_stop,
    check_volatility_position_size,
    count_distribution_days,
)


def parse_earnings_date(raw):
    if raw is None:
        return None
    if isinstance(raw, float) and pd.isna(raw):
        return None
    if isinstance(raw, str):
        if not raw or raw in ('-', 'N/A', ''):
            return None
        parts = raw.split()
        if len(parts) < 2:
            return None
        today = date.today()
        for year in [today.year, today.year + 1]:
            try:
                d = datetime.strptime(f"{parts[0]} {parts[1]} {year}", "%b %d %Y").date()
                if d >= today - timedelta(days=180):
                    return d
            except ValueError:
                continue
    return None


st.set_page_config(page_title="Minervini", page_icon="🎯", layout="wide")

st.title("🎯 Mark Minervini — 4 Liste Stratejisi")
st.caption("SEPA: Specific Entry Point Analysis | 4 kademeli liste yapisi")

USER_EMAIL = "ferit@quanfina.local"


@st.cache_data(ttl=3600)
def cached_setup_types() -> list:
    """setup_types lookup'unu cache'ler (1 saat TTL).

    Returns:
        [(1, 'VCP'), (2, 'CWH'), ...]
    """
    return get_setup_types()


@st.cache_data(ttl=60)
def get_user_id() -> int:
    """Ferit'in user_id'sini dondurur (cache'li)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE email = %s", (USER_EMAIL,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 1


@st.cache_data(ttl=60)
def load_list_data(list_code: str, user_id: int) -> pd.DataFrame:
    """symbol_lists tablosundan liste hisselerini cek + minervini_scans ile JOIN.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'
        user_id: Kullanici ID'si

    Returns:
        DataFrame: list metadata + scanner verisi (en son scan tarihi)
        Sutunlar:
        - symbol_lists: id, symbol, day_added, note, pivot_price, pullback_health, tt_score
        - minervini_scans (LEFT JOIN, en son scan): company, sector, price, change_pct,
          grade, rs_ibd, rs_12m, ma200_slope, pct_from_high, volume,
          eps_qoq, sales_qoq, market_cap, days_to_earnings, confirmations, violations
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(scan_date) FROM minervini_scans")
    last_scan_date = cur.fetchone()[0]

    cur.execute(
        "SELECT "
        "    sl.id, sl.symbol, sl.day_added, sl.note, "
        "    sl.pivot_price, sl.pullback_health, sl.tt_score, "
        "    sl.setup_type_id, st.name AS setup_type_name, "
        "    s.company, s.sector, s.price, s.change_pct, "
        "    s.grade, s.rs_ibd, s.rs_12m, "
        "    s.ma200_slope, s.high52, s.volume, "
        "    s.eps_qoq, s.sales_qoq, s.market_cap, "
        "    s.earnings_date, s.confirmations, s.violations, "
        "    s.sma50, s.atr14, NULL AS price_volume_history "
        "FROM symbol_lists sl "
        "LEFT JOIN setup_types st ON st.id = sl.setup_type_id "
        "LEFT JOIN minervini_scans s "
        "    ON s.ticker = sl.symbol AND s.scan_date = %s "
        "WHERE sl.user_id = %s AND sl.list_type = %s AND sl.strategy = 'minervini' "
        "ORDER BY sl.symbol",
        (last_scan_date, user_id, list_code)
    )
    rows = cur.fetchall()
    cols = [
        "id", "symbol", "day_added", "note",
        "pivot_price", "pullback_health", "tt_score",
        "setup_type_id", "setup_type_name",
        "company", "sector", "price", "change_pct",
        "grade", "rs_ibd", "rs_12m",
        "ma200_slope", "high52", "volume",
        "eps_qoq", "sales_qoq", "market_cap",
        "earnings_date", "confirmations", "violations",
        "sma50", "atr14", "price_volume_history",
    ]
    conn.close()
    df = pd.DataFrame(rows, columns=cols)
    df["pct_from_high"] = (
        (df["price"] - df["high52"]) / df["high52"] * 100
    ).where(df["high52"].fillna(0) > 0)
    df["days_to_earnings"] = df["earnings_date"].apply(
        lambda x: (parse_earnings_date(x) - date.today()).days
        if parse_earnings_date(x) else None
    )
    return df


def render_list_tab(list_code: str) -> None:
    """Bir liste tab'inin icerigini cizer.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'
    """
    meta = get_list_meta(list_code)
    expected_cols = get_columns_for_list(list_code)

    st.subheader(meta["label"])
    st.caption(meta["description"])

    user_id = get_user_id()
    df = load_list_data(list_code, user_id)

    col1, col2, col3 = st.columns(3)
    col1.metric("Hisse Sayisi", len(df))
    col2.metric("Hedef Sutun", meta["default_column_count"])
    col3.metric("Liste Sirasi", meta["sort_order"])

    if len(df) == 0:
        st.info(
            "**Bu liste henuz bos.**\n\n"
            "Sprint 4.3b'de test verisi eklenebilir:\n"
            "`venv\\Scripts\\python.exe scripts\\seed_symbol_lists.py`"
        )
        with st.expander("Bu liste icin hedef sutun yapisi"):
            label_data = [{"DB Kolon": c, "Display Label": get_label(c)} for c in expected_cols]
            st.dataframe(pd.DataFrame(label_data), use_container_width=True, hide_index=True)
        return

    # Liste tipine gore sutunlari filtrele
    available_cols = [c for c in expected_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # Sutun isimlerini Turkce label'a cevir
    display_df.columns = [get_label(c) for c in available_cols]

    column_config = {}
    if "FIYAT" in display_df.columns:
        column_config["FIYAT"] = st.column_config.NumberColumn(format="$%.2f")
    if "DEGISIM" in display_df.columns:
        column_config["DEGISIM"] = st.column_config.NumberColumn(format="%.2f%%")
    if "RS (IBD)" in display_df.columns:
        column_config["RS (IBD)"] = st.column_config.NumberColumn(format="%.0f")
    if "RS (12A)" in display_df.columns:
        column_config["RS (12A)"] = st.column_config.NumberColumn(format="%.0f")
    if "MA200 SLOPE" in display_df.columns:
        column_config["MA200 SLOPE"] = st.column_config.NumberColumn(format="%.2f")
    if "% YUKSEK MESAFE" in display_df.columns:
        column_config["% YUKSEK MESAFE"] = st.column_config.NumberColumn(format="%.1f%%")
    if "HACIM" in display_df.columns:
        column_config["HACIM"] = st.column_config.NumberColumn(format="%d")
    if "PIYASA DEGERI (M)" in display_df.columns:
        column_config["PIYASA DEGERI (M)"] = st.column_config.NumberColumn(format="$%.0fM")
    if "EPS Q/Q" in display_df.columns:
        column_config["EPS Q/Q"] = st.column_config.NumberColumn(format="%.1f%%")
    if "SALES Q/Q" in display_df.columns:
        column_config["SALES Q/Q"] = st.column_config.NumberColumn(format="%.1f%%")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )

    # Sprint 4.7e.2/4.7e.3 — 50-MA Trail Stop + Volatility + Distribution Days (Focus/Buy tabları)
    if list_code in ("focus", "buy") and not df.empty:
        has_signal_data = ("sma50" in df.columns and "atr14" in df.columns
                           and "price" in df.columns
                           and df["sma50"].notna().any())
        if has_signal_data:
            signal_rows = []
            for _, r in df.iterrows():
                ticker = r.get("symbol", "?")
                price  = r.get("price")
                sma50  = r.get("sma50")
                atr14  = r.get("atr14")
                if pd.isna(price) or pd.isna(sma50):
                    continue
                ma_rec = check_50ma_trail_stop(
                    current_price=float(price),
                    ma50=float(sma50),
                    invest_type="LONG",
                    is_climax_run=False,
                )
                vol_rec = None
                if not pd.isna(atr14):
                    vol_rec = check_volatility_position_size(
                        atr=float(atr14),
                        current_price=float(price),
                        proposed_position_pct=10.0,
                    )
                # Sprint 4.7e.3 — Distribution Days
                dist_rec = None
                pvh_raw = r.get("price_volume_history")
                if pvh_raw:
                    try:
                        pvh_list = json.loads(pvh_raw) if isinstance(pvh_raw, str) else pvh_raw
                        pvh_tuples = [(d["date"], d["close"], d["volume"]) for d in pvh_list]
                        dist_rec = count_distribution_days(pvh_tuples, lookback_days=20)
                    except Exception:
                        dist_rec = None

                has_warning = (
                    ma_rec.severity != "OK"
                    or (vol_rec and vol_rec.severity != "OK")
                    or (dist_rec and dist_rec.severity != "OK")
                )
                if has_warning:
                    signal_rows.append({
                        "Ticker":     ticker,
                        "50-MA":      ma_rec.severity if ma_rec.severity != "OK" else "—",
                        "50-MA Not":  ma_rec.message  if ma_rec.severity != "OK" else "",
                        "ATR":        vol_rec.severity if vol_rec and vol_rec.severity != "OK" else "—",
                        "ATR Not":    vol_rec.message  if vol_rec and vol_rec.severity != "OK" else "",
                        "Dağıtım":    (f"{int(dist_rec.suggested_value)} gün" if dist_rec and dist_rec.severity != "OK" else "—"),
                        "Dağıtım Not": dist_rec.message if dist_rec and dist_rec.severity != "OK" else "",
                    })
            if signal_rows:
                with st.expander(f"🚨 Sinyaller ({len(signal_rows)} hisse)", expanded=False):
                    st.caption(
                        "50-MA Trail Stop, Volatility (ATR) ve Dağıtım Günleri uyarıları — "
                        "yalnızca OK olmayan hisseler. Veri: minervini_scans."
                    )
                    st.dataframe(pd.DataFrame(signal_rows), use_container_width=True, hide_index=True)
            else:
                with st.expander("✅ Sinyaller (tümü OK)", expanded=False):
                    st.caption("50-MA, ATR ve Dağıtım Günleri uyarısı olan hisse yok.")
        else:
            with st.expander("⏳ Sinyaller (veri bekleniyor)", expanded=False):
                st.caption(
                    "sma50 / atr14 henüz taranmamış. "
                    "Sprint 4.7e.1 sonrası yeni tarama tetiklendiğinde dolar."
                )

    with st.expander("Liste metadata (id, eklenme tarihi, notlar, pivot, vb.)"):
        meta_cols = ["id", "symbol", "day_added", "setup_type_name", "note",
                     "pivot_price", "pullback_health", "tt_score"]
        meta_available = [c for c in meta_cols if c in df.columns]
        st.dataframe(df[meta_available], use_container_width=True, hide_index=True)

    # ── CRUD UI ──────────────────────────────────────────────────────────
    with st.expander("➕ Hisse Ekle"):
        with st.form(key=f"add_form_{list_code}", clear_on_submit=True):
            col_sym, col_note = st.columns([1, 2])
            new_symbol = col_sym.text_input(
                "Ticker", placeholder="AAPL", key=f"add_sym_{list_code}"
            )
            new_note = col_note.text_input(
                "Not (opsiyonel)", placeholder="VCP setup, breakout watch",
                key=f"add_note_{list_code}"
            )
            col_setup, col_pivot = st.columns([1, 1])
            setup_pairs = cached_setup_types()
            setup_options = {"— Seçilmedi —": None}
            for sid, sname in setup_pairs:
                setup_options[sname] = sid
            sel_setup_label = col_setup.selectbox(
                "Setup Tipi (opsiyonel)",
                list(setup_options.keys()),
                key=f"add_setup_{list_code}"
            )
            sel_pivot = col_pivot.number_input(
                "Pivot Fiyat (opsiyonel)",
                min_value=0.0,
                step=0.01,
                format="%.2f",
                key=f"add_pivot_{list_code}"
            )
            submitted = st.form_submit_button("Ekle", type="primary")
        if submitted:
            sym_clean = (new_symbol or "").upper().strip()
            if not sym_clean:
                st.error("Ticker bos olamaz.")
            else:
                ok = add_to_list(
                    user_id, sym_clean, list_code,
                    note=new_note,
                    setup_type_id=setup_options[sel_setup_label],
                    pivot_price=sel_pivot if sel_pivot > 0 else None
                )
                if ok:
                    st.success(f"✅ {sym_clean} → {meta['label']} listesine eklendi.")
                    load_list_data.clear()
                    st.rerun()
                else:
                    st.warning(f"⚠️ {sym_clean} bu listede zaten var.")

    if len(df) > 0:
        with st.expander("🗑️ Listeden Cikar"):
            symbols_in_list = df["symbol"].tolist()
            to_remove = st.selectbox(
                "Silinecek hisse", symbols_in_list, key=f"remove_sel_{list_code}"
            )
            col_btn1, _ = st.columns([1, 4])
            if col_btn1.button("Sil", key=f"remove_btn_{list_code}", type="secondary"):
                ok = remove_from_list(user_id, to_remove, list_code)
                if ok:
                    st.success(f"✅ {to_remove} listeden silindi.")
                    load_list_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ {to_remove} bulunamadi.")

    # ── Watch'a ozel: On Deck'e Promote ─────────────────────────────────
    if list_code == "watch" and len(df) > 0:
        with st.expander("⬆️ On Deck'e Tasi"):
            with st.form(key=f"promote_form_{list_code}", clear_on_submit=True):
                to_promote = st.selectbox(
                    "Hisse",
                    df["symbol"].tolist(),
                    key=f"promote_sel_{list_code}"
                )
                col_s, col_p = st.columns([1, 1])
                promote_pairs = cached_setup_types()
                promote_options = {"— Seçilmedi —": None}
                for sid, sname in promote_pairs:
                    promote_options[sname] = sid
                promote_setup_label = col_s.selectbox(
                    "Setup Tipi",
                    list(promote_options.keys()),
                    key=f"promote_setup_{list_code}"
                )
                promote_pivot = col_p.number_input(
                    "Pivot Fiyat",
                    min_value=0.0,
                    step=0.01,
                    format="%.2f",
                    key=f"promote_pivot_{list_code}"
                )
                promote_submitted = st.form_submit_button(
                    "⬆️ On Deck'e Tasi", type="primary"
                )
            if promote_submitted:
                ok = promote_to_list(
                    user_id, to_promote, "watch", "on_deck",
                    setup_type_id=promote_options[promote_setup_label],
                    pivot_price=promote_pivot if promote_pivot > 0 else None
                )
                if ok:
                    st.success(f"✅ {to_promote} → On Deck listesine tasindi.")
                    load_list_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ {to_promote} tasinamadi.")


# 4 dis tab
tab_watch, tab_on_deck, tab_focus, tab_buy = st.tabs([
    "⭐ Watch (100+)",
    "🎯 On Deck (30-40)",
    "🔥 Focus (5-15)",
    "💰 Buy (3-5)",
])

with tab_watch:
    render_list_tab("watch")

with tab_on_deck:
    render_list_tab("on_deck")

with tab_focus:
    render_list_tab("focus")

with tab_buy:
    render_list_tab("buy")


st.divider()
st.caption(
    "Sprint 4.2 iskelet | symbol_lists tablosu | "
    "Eski 7+4 tab yapisi: 'Minervini Old' sayfasi"
)
