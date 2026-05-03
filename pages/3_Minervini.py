import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import date, datetime, timedelta

st.set_page_config(page_title="Minervini Trend Template", layout="wide")


def parse_earnings_date(raw: str):
    if raw is None or isinstance(raw, float) and pd.isna(raw):
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

st.markdown("""
<style>
    .metric-box { background-color: #1e293b; padding: 15px; border-radius: 10px; border: 1px solid #334155; text-align: center; }
</style>
""", unsafe_allow_html=True)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "quanfina.db")

@st.cache_data(ttl=300)
def load_scan(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM minervini_scans WHERE scan_date = ? ORDER BY ma200_slope DESC",
        conn, params=(scan_date,)
    )
    conn.close()
    return df

def get_available_dates():
    conn = sqlite3.connect(DB_PATH)
    dates = pd.read_sql_query(
        "SELECT DISTINCT scan_date FROM minervini_scans ORDER BY scan_date DESC",
        conn
    )
    conn.close()
    return dates["scan_date"].tolist()

@st.cache_data(ttl=300)
def load_fundamental_scan(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            f.ticker, f.company, f.sector, f.industry,
            f.price, f.change_pct, f.volume, f.market_cap, f.pe,
            f.ma200_slope, f.high52, f.confirmations, f.violations,
            s.eps_qoq, s.sales_qoq, s.grade, s.earnings_date,
            COALESCE(f.rs_ibd,       s.rs_ibd)       AS rs_ibd,
            COALESCE(f.rs_12m,       s.rs_12m)       AS rs_12m,
            COALESCE(f.rs_20d,       s.rs_20d)       AS rs_20d,
            COALESCE(f.rs_50d,       s.rs_50d)       AS rs_50d,
            COALESCE(f.rs_200d,      s.rs_200d)      AS rs_200d,
            COALESCE(f.rs_mansfield, s.rs_mansfield) AS rs_mansfield
        FROM minervini_fundamental_scans f
        LEFT JOIN minervini_scans s ON f.ticker = s.ticker AND f.scan_date = s.scan_date
        WHERE f.scan_date = ?
        ORDER BY COALESCE(f.rs_ibd, s.rs_ibd) DESC
    """, conn, params=(scan_date,))
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_fundamental_only(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT f.*, s.earnings_date
        FROM minervini_fundamental_only f
        LEFT JOIN minervini_scans s ON f.ticker = s.ticker AND f.scan_date = s.scan_date
        WHERE f.scan_date = ?
        ORDER BY f.market_cap DESC
    """, conn, params=(scan_date,))
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_52w_high(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT f.*, s.earnings_date
        FROM minervini_52w_high f
        LEFT JOIN minervini_scans s ON f.ticker = s.ticker AND f.scan_date = s.scan_date
        WHERE f.scan_date = ?
        ORDER BY f.ma200_slope DESC
    """, conn, params=(scan_date,))
    conn.close()
    return df

def get_watchlist_tickers():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT ticker FROM minervini_watchlist ORDER BY ticker", conn)
    conn.close()
    return df["ticker"].tolist()

def add_tickers_to_watchlist(tickers):
    conn = sqlite3.connect(DB_PATH)
    today = str(date.today())
    for ticker in tickers:
        conn.execute(
            "INSERT OR IGNORE INTO minervini_watchlist (ticker, added_date) VALUES (?, ?)",
            (ticker, today),
        )
    conn.commit()
    conn.close()

def save_watchlist(selected_tickers):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = str(date.today())
    if selected_tickers:
        placeholders = ','.join('?' * len(selected_tickers))
        c.execute(f"DELETE FROM minervini_watchlist WHERE ticker NOT IN ({placeholders})",
                  selected_tickers)
        for ticker in selected_tickers:
            c.execute("INSERT OR IGNORE INTO minervini_watchlist (ticker, added_date) VALUES (?,?)",
                      (ticker, today))
    else:
        c.execute("DELETE FROM minervini_watchlist")
    conn.commit()
    conn.close()

@st.cache_data(ttl=60)
def load_watchlist_data(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            w.ticker,
            COALESCE(s.company,     h.company,     f.company)     AS company,
            COALESCE(s.sector,      h.sector,      f.sector)      AS sector,
            COALESCE(s.industry,    h.industry,    f.industry)    AS industry,
            COALESCE(s.price,       h.price,       f.price)       AS price,
            COALESCE(s.change_pct,  h.change_pct,  f.change_pct)  AS change_pct,
            COALESCE(s.volume,      h.volume,      f.volume)      AS volume,
            COALESCE(s.market_cap,  h.market_cap,  f.market_cap)  AS market_cap,
            COALESCE(s.ma200_slope, h.ma200_slope, f.ma200_slope) AS ma200_slope,
            COALESCE(s.high52,      h.high52,      f.high52)      AS high52,
            COALESCE(s.earnings_date, NULL)                    AS earnings_date,
            COALESCE(s.eps_qoq,        h.eps_qoq,        f.eps_qoq)        AS eps_qoq,
            COALESCE(s.sales_qoq,      h.sales_qoq,      f.sales_qoq)      AS sales_qoq,
            COALESCE(s.grade,          h.grade,          f.grade)          AS grade,
            COALESCE(s.confirmations,  h.confirmations,  f.confirmations)  AS confirmations,
            COALESCE(s.violations,     h.violations,     f.violations)     AS violations,
            COALESCE(s.rs_ibd,         h.rs_ibd,         f.rs_ibd)         AS rs_ibd,
            COALESCE(s.rs_12m,         h.rs_12m,         f.rs_12m)         AS rs_12m,
            COALESCE(s.rs_20d,         h.rs_20d,         f.rs_20d)         AS rs_20d,
            COALESCE(s.rs_50d,         h.rs_50d,         f.rs_50d)         AS rs_50d,
            COALESCE(s.rs_200d,        h.rs_200d,        f.rs_200d)        AS rs_200d,
            COALESCE(s.rs_mansfield,   h.rs_mansfield,   f.rs_mansfield)   AS rs_mansfield
        FROM minervini_watchlist w
        LEFT JOIN minervini_scans s
            ON s.ticker = w.ticker AND s.scan_date = ?
        LEFT JOIN minervini_52w_high h
            ON h.ticker = w.ticker AND h.scan_date = ?
        LEFT JOIN minervini_fundamental_only f
            ON f.ticker = w.ticker AND f.scan_date = ?
        ORDER BY w.ticker
    """, conn, params=(scan_date, scan_date, scan_date))
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_grade_changes(scan_date):
    _GRADE_VALUE = {"D": 1, "C": 2, "B": 3, "A": 4}
    conn = sqlite3.connect(DB_PATH)
    prev_row = pd.read_sql_query(
        "SELECT DISTINCT scan_date FROM minervini_scans WHERE scan_date < ? ORDER BY scan_date DESC LIMIT 1",
        conn, params=(scan_date,)
    )
    _EMPTY_COLS = [
        "ticker", "company", "sector", "industry", "price", "change_pct",
        "volume", "market_cap", "ma200_slope", "high52", "eps_qoq", "sales_qoq",
        "grade", "confirmations", "violations", "grade_change", "jump_size",
    ]
    if prev_row.empty:
        conn.close()
        return pd.DataFrame(columns=_EMPTY_COLS)
    prev_date = prev_row["scan_date"].iloc[0]
    df = pd.read_sql_query("""
        SELECT
            curr.ticker, curr.company, curr.sector, curr.industry,
            curr.price, curr.change_pct, curr.volume, curr.market_cap,
            curr.ma200_slope, curr.high52, curr.eps_qoq, curr.sales_qoq,
            curr.grade AS curr_grade, prev.grade AS prev_grade,
            curr.earnings_date, curr.confirmations, curr.violations
        FROM minervini_scans curr
        JOIN minervini_scans prev
            ON curr.ticker = prev.ticker AND prev.scan_date = ?
        WHERE curr.scan_date = ?
          AND curr.grade IS NOT NULL
          AND prev.grade IS NOT NULL
    """, conn, params=(prev_date, scan_date))
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)
    df["curr_value"]  = df["curr_grade"].map(_GRADE_VALUE)
    df["prev_value"]  = df["prev_grade"].map(_GRADE_VALUE)
    df["jump_size"]   = df["curr_value"] - df["prev_value"]
    df = df[df["jump_size"] > 0].copy()
    df["grade_change"] = df.apply(
        lambda r: f"{r['prev_grade']} → {r['curr_grade']} (+{int(r['jump_size'])})", axis=1
    )
    df["grade"] = df["curr_grade"]
    return df.sort_values(["jump_size", "curr_value"], ascending=[False, False]).reset_index(drop=True)

@st.cache_data(ttl=300)
def compute_sector_leaders(scan_date):
    conn = sqlite3.connect(DB_PATH)
    raw = pd.read_sql_query(
        "SELECT sector, grade FROM minervini_scans WHERE scan_date = ?",
        conn, params=(scan_date,)
    )
    conn.close()
    if raw.empty:
        return pd.DataFrame(columns=["Sektör", "A", "B", "C", "D", "Toplam", "Skor"])
    grp = raw.groupby("sector")["grade"].value_counts().unstack(fill_value=0)
    for g in ["A", "B", "C", "D"]:
        if g not in grp.columns:
            grp[g] = 0
    grp = grp[["A", "B", "C", "D"]]
    grp["Toplam"] = grp.sum(axis=1)
    grp["Skor"]   = grp["A"]*4 + grp["B"]*3 + grp["C"]*2 + grp["D"]*1
    grp = grp.sort_values("Skor", ascending=False).reset_index()
    grp.rename(columns={"sector": "Sektör"}, inplace=True)
    return grp

_VARSAYILAN = "Varsayılan (Tabloya özel)"

_DEFAULT_SORT = {
    "super":     [("rs_ibd", False)],
    "52w":       [("pct_from_high", False)],
    "grade_up":  [("jump_size", False), ("grade", True)],
    "trend":     [("grade", True), ("ma200_slope", False)],
    "fund_only": [("eps_qoq", False)],
    "watchlist": [("ticker", True)],
}

def apply_filters(data, sektor, grade_list, siralama, arama, table_type="super",
                  rs_min=0, rs_kolonu="rs_ibd"):
    if data.empty:
        return data
    d = data.copy()
    if sektor != "Tümü":
        d = d[d["sector"] == sektor]
    if grade_list:
        d = d[d["grade"].isin(grade_list)]
    if arama:
        q = arama.strip().upper()
        mask = (d["ticker"].str.upper().str.contains(q, na=False) |
                d["company"].str.upper().str.contains(q, na=False))
        d = d[mask]
    if rs_min > 0 and rs_kolonu in d.columns:
        d = d[d[rs_kolonu].fillna(0) >= rs_min]
    if siralama == _VARSAYILAN:
        sort_keys = _DEFAULT_SORT.get(table_type, [("rs_ibd", False)])
    else:
        _SORT_MAP = {
            "RS IBD (azalan)":      [("rs_ibd",      False)],
            "MA200 Slope (azalan)": [("ma200_slope", False)],
            "EPS Q/Q (azalan)":     [("eps_qoq",     False)],
            "Sales Q/Q (azalan)":   [("sales_qoq",   False)],
            "Hacim (azalan)":       [("volume",       False)],
            "Ticker (A-Z)":         [("ticker",       True)],
        }
        sort_keys = _SORT_MAP.get(siralama, [("rs_ibd", False)])
    cols = [c for c, _ in sort_keys if c in d.columns]
    ascs = [a for c, a in sort_keys if c in d.columns]
    if cols:
        d = d.sort_values(cols, ascending=ascs, na_position="last")
    return d

# --- SAYFA ---
st.title("📈 Minervini Trend Template")
st.caption("Günlük tarama sonuçları — Finviz Elite + 8 Kural")

dates = get_available_dates()

if not dates:
    st.warning("Henüz tarama yapılmamış. scanner.py'yi çalıştır.")
    st.stop()

selected_date = st.selectbox("Tarama tarihi:", dates)

df           = load_scan(selected_date)
df_fund      = load_fundamental_scan(selected_date)
df_fund_only = load_fundamental_only(selected_date)
df_52w       = load_52w_high(selected_date)
wl_tickers   = get_watchlist_tickers()
df_wl        = load_watchlist_data(selected_date)
df_grade_up  = load_grade_changes(selected_date)

for _df in [df, df_fund, df_fund_only, df_52w, df_wl, df_grade_up]:
    if "high52" in _df.columns:
        _df["pct_from_high"] = (((_df["price"] - _df["high52"]) / _df["high52"]) * 100).where(_df["high52"] > 0)
    if "earnings_date" in _df.columns:
        _df["days_to_earnings"] = _df["earnings_date"].apply(
            lambda x: (parse_earnings_date(x) - date.today()).days if parse_earnings_date(x) else None
        )
    for _sig_col in ["confirmations", "violations"]:
        if _sig_col in _df.columns:
            _df[_sig_col] = _df[_sig_col].fillna("").str.replace(",", " · ")

if df.empty:
    st.warning("Bu tarihe ait veri yok.")
    st.stop()

# --- FİLTRE PANELİ ---
with st.expander("🔍 Filtreler", expanded=False):
    all_sectors = sorted(
        set(df["sector"].dropna()) |
        set(df_fund["sector"].dropna()) |
        set(df_fund_only["sector"].dropna()) |
        set(df_52w["sector"].dropna()) |
        (set(df_grade_up["sector"].dropna()) if not df_grade_up.empty else set())
    )
    c1, c2, c3, c4 = st.columns(4)
    secim_sektor   = c1.selectbox("Sektör", ["Tümü"] + all_sectors)
    secim_grade    = c2.multiselect("Grade", ["A", "B", "C", "D"], default=["A", "B", "C", "D"])
    secim_siralama = c3.selectbox("Sıralama", [
        "Varsayılan (Tabloya özel)",
        "RS IBD (azalan)",
        "MA200 Slope (azalan)",
        "EPS Q/Q (azalan)",
        "Sales Q/Q (azalan)",
        "Hacim (azalan)",
        "Ticker (A-Z)",
    ])
    secim_arama = c4.text_input("Hisse ara (TICKER / Şirket)")
    st.divider()
    st.markdown("**📊 RS Rating Filtreleri**")
    rs_c1, rs_c2, rs_c3 = st.columns(3)
    secim_rs_kolonlar = rs_c1.multiselect(
        "Görüntülenecek RS skorları",
        ["rs_ibd", "rs_12m", "rs_20d", "rs_50d", "rs_200d"],
        default=["rs_ibd"],
    )
    secim_rs_min = rs_c2.slider("Minimum RS skoru (tüm tablolar)", 0, 99, 0)
    secim_rs_col = rs_c3.radio(
        "RS filtresi için skor",
        ["rs_ibd", "rs_12m", "rs_20d", "rs_50d", "rs_200d"],
        horizontal=True,
    )
    st.divider()
    secim_super_rs80 = st.checkbox(
        "🌟 Süper Performans: RS ≥ 80 göster",
        value=True,
    )

# Filtrelenmiş varyantlar
df_pass_f      = apply_filters(df[df["passed"] == 1], secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="trend",     rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
df_partial_f   = apply_filters(df[df["passed"] == 0], secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="trend",     rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
df_fund_f      = apply_filters(df_fund,      secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="super",     rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
if secim_super_rs80 and "rs_ibd" in df_fund_f.columns:
    if df_fund_f["rs_ibd"].notna().any():
        df_fund_f = df_fund_f[df_fund_f["rs_ibd"].fillna(0) >= 80]
df_fund_only_f = apply_filters(df_fund_only, secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="fund_only", rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
df_52w_f       = apply_filters(df_52w,       secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="52w",       rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
df_wl_f        = apply_filters(df_wl,        secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="watchlist", rs_min=secim_rs_min, rs_kolonu=secim_rs_col)
df_grade_up_f  = apply_filters(df_grade_up,  secim_sektor, secim_grade, secim_siralama, secim_arama, table_type="grade_up", rs_min=secim_rs_min, rs_kolonu=secim_rs_col)

# --- SEKTÖR LİDERLERİ ---
df_sector = compute_sector_leaders(selected_date)

with st.expander("🏆 Sektör Liderleri", expanded=False):
    if df_sector.empty:
        st.info("Veri yok.")
    else:
        lider = df_sector.iloc[0]
        zayif = df_sector.iloc[-1]
        m1, m2, m3 = st.columns(3)
        m1.metric("🥇 En Güçlü Sektör", lider["Sektör"], f"Skor: {int(lider['Skor'])}")
        m2.metric("📉 En Zayıf Sektör", zayif["Sektör"], f"Skor: {int(zayif['Skor'])}")
        m3.metric("⭐ Lider Sektör A-Grade", int(lider["A"]))

        def _sector_row_style(row):
            avg = row["Skor"] / row["Toplam"] if row["Toplam"] > 0 else 0
            if avg >= 3.5:
                css = "background-color: #d1fae5; color: black"
            elif avg >= 2.5:
                css = "background-color: #fef3c7; color: black"
            elif avg >= 1.5:
                css = "background-color: #fed7aa; color: black"
            else:
                css = "background-color: #e5e7eb; color: black"
            return [css] * len(row)

        styled = df_sector.style.apply(_sector_row_style, axis=1)
        st.dataframe(
            styled,
            use_container_width=True,
            height=min(len(df_sector) * 35 + 50, 500),
            hide_index=True,
            column_config={
                "Sektör": st.column_config.TextColumn("Sektör"),
                "A":      st.column_config.NumberColumn("A", format="%d"),
                "B":      st.column_config.NumberColumn("B", format="%d"),
                "C":      st.column_config.NumberColumn("C", format="%d"),
                "D":      st.column_config.NumberColumn("D", format="%d"),
                "Toplam": st.column_config.NumberColumn("Toplam", format="%d"),
                "Skor":   st.column_config.NumberColumn("Skor", format="%d"),
            },
        )

st.caption(
    f"📊 Filtreli sonuçlar: "
    f"Süper Performans **{len(df_fund_f)}/{len(df_fund)}** · "
    f"52H Yüksek **{len(df_52w_f)}/{len(df_52w)}** · "
    f"TPR Moving Up **{len(df_grade_up_f)}/{len(df_grade_up)}** · "
    f"Trend Template **{len(df_pass_f)}/{len(df[df['passed']==1])}** · "
    f"Sadece Temel **{len(df_fund_only_f)}/{len(df_fund_only)}** · "
    f"Watch List **{len(df_wl_f)}/{len(df_wl)}**"
)

def add_link_columns(df):
    df = df.copy()
    df["tv_url"] = df["ticker"].apply(
        lambda t: f"https://www.tradingview.com/chart/?symbol={t}"
    )
    df["fv_url"] = df["ticker"].apply(
        lambda t: f"https://finviz.com/quote.ashx?t={t}"
    )
    return df

_GRADE_CSS = {
    "A": "background-color: #d1fae5; color: black",
    "B": "background-color: #fef3c7; color: black",
    "C": "background-color: #fed7aa; color: black",
    "D": "background-color: #e5e7eb; color: black",
}

def style_grade(df):
    def _row(row):
        css = _GRADE_CSS.get(str(row["GRADE"]), "")
        return [css] * len(row)
    return df.style.apply(_row, axis=1)

_RS_COL_NAMES = ["RS (IBD)", "RS (12A)", "RS (20G)", "RS (50G)", "RS (200G)"]

def _rs_cell_style(val):
    try:
        v = int(val)
    except (TypeError, ValueError):
        return ""
    if v >= 90:
        return "background-color: #14532d; color: white"
    if v >= 80:
        return "background-color: #16a34a; color: white"
    if v >= 70:
        return "background-color: #86efac; color: black"
    if v >= 50:
        return ""
    return "background-color: #fca5a5; color: black"

def add_rs_styling(styled, cols):
    present = [c for c in _RS_COL_NAMES if c in cols]
    if present:
        styled = styled.map(_rs_cell_style, subset=present)
    return styled

def table_height(df):
    return min(len(df) * 35 + 50, 800)

def add_to_watchlist_button(df_filtered, key):
    available = set(df_filtered.columns) | {"tv_url", "fv_url"}
    safe_cols = [c for c in show_cols if c in available]
    df_display = add_link_columns(df_filtered)[safe_cols].rename(columns=col_rename)
    styled = add_rs_styling(style_grade(df_display), list(df_display.columns))
    event = st.dataframe(
        styled,
        hide_index=True,
        use_container_width=True,
        column_config=col_config,
        height=table_height(df_filtered),
        selection_mode="multi-row",
        on_select="rerun",
        key=key,
    )
    selected_rows = event.selection.rows
    tickers = df_filtered.iloc[selected_rows]["ticker"].tolist() if selected_rows else []
    label = f"⭐ Seçili hisseleri Watch List'e ekle ({len(tickers)})" if tickers else "⭐ Watch List'e ekle"
    if st.button(label, key=f"{key}_btn", disabled=not tickers):
        add_tickers_to_watchlist(tickers)
        load_watchlist_data.clear()
        st.success(f"{len(tickers)} hisse Watch List'e eklendi.")
        st.rerun()

col_rename = {
    "ticker": "TICKER", "company": "ŞİRKET", "sector": "SEKTÖR",
    "industry": "SEKTÖR ALTI", "price": "FİYAT", "change_pct": "DEĞİŞİM",
    "volume": "HACİM", "market_cap": "PİYASA DEĞERİ (M)",
    "ma200_slope": "MA200 SLOPE", "pct_from_high": "% YÜKSEK MESAFE",
    "days_to_earnings": "BİLANÇO (gün)",
    "eps_qoq": "EPS Q/Q", "sales_qoq": "SALES Q/Q", "grade": "GRADE",
    "confirmations": "✅ POZİTİF", "violations": "❌ NEGATİF",
    "rs_ibd":       "RS (IBD)", "rs_12m":  "RS (12A)",
    "rs_20d":       "RS (20G)", "rs_50d":  "RS (50G)",
    "rs_200d":      "RS (200G)", "rs_mansfield": "Mansfield",
    "grade_change": "GRADE DEĞİŞİM",
    "tv_url": "📊 TV", "fv_url": "📈 FV",
}

col_config = {
    "FİYAT": st.column_config.NumberColumn(format="$%.2f"),
    "MA200 SLOPE": st.column_config.NumberColumn(format="%.4f"),
    "% YÜKSEK MESAFE": st.column_config.NumberColumn(format="%.1f%%"),
    "BİLANÇO (gün)": st.column_config.NumberColumn(format="%d"),
    "HACİM": st.column_config.NumberColumn(format="%d"),
    "EPS Q/Q": st.column_config.NumberColumn(format="%.1f"),
    "SALES Q/Q": st.column_config.NumberColumn(format="%.1f"),
    "✅ POZİTİF": st.column_config.TextColumn(width="medium"),
    "❌ NEGATİF": st.column_config.TextColumn(width="medium"),
    "RS (IBD)":  st.column_config.NumberColumn(format="%d"),
    "RS (12A)":  st.column_config.NumberColumn(format="%d"),
    "RS (20G)":  st.column_config.NumberColumn(format="%d"),
    "RS (50G)":  st.column_config.NumberColumn(format="%d"),
    "RS (200G)": st.column_config.NumberColumn(format="%d"),
    "Mansfield": st.column_config.NumberColumn(format="%.3f"),
    "GRADE DEĞİŞİM": st.column_config.TextColumn(),
    "📊 TV": st.column_config.LinkColumn(display_text="🔗 Aç"),
    "📈 FV": st.column_config.LinkColumn(display_text="🔗 Aç"),
}

_rs_show  = secim_rs_kolonlar + ["rs_mansfield"]
show_cols = (
    ["ticker", "company", "sector", "industry", "price", "change_pct",
     "volume", "market_cap", "ma200_slope", "pct_from_high", "days_to_earnings",
     "eps_qoq", "sales_qoq", "grade"]
    + _rs_show
    + ["confirmations", "violations", "grade_change", "tv_url", "fv_url"]
)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🌟 Süper Performans",
    "🚀 52 Hafta Yüksek",
    "📈 TPR Moving Up",
    "✅ Trend Template 8/8",
    "📊 Sadece Temel",
    "⭐ Watch List",
])

with tab1:
    st.subheader("⭐ Süper Performans Adayları (Teknik + EPS + Sales)")
    st.caption("Trend Template 8/8 + EPS Q/Q > %25 + Sales Q/Q > %25")
    if df_fund.empty:
        st.warning("Bu tarihe ait fundamental veri yok.")
    else:
        f1, f2 = st.columns(2)
        f1.metric("Süper Performans Adayı", len(df_fund_f))
        f2.metric("Teknik Listeden Oranı", f"{len(df_fund)/len(df)*100:.1f}%")
        add_to_watchlist_button(df_fund_f, "tab1")

with tab2:
    st.subheader("🚀 52 Hafta Yüksek (Yeni Liderler)")
    st.caption("Fiyat > $10 + Hacim > 500K + 52W yeni yüksek yapıyor")
    if df_52w.empty:
        st.warning("Bu tarihe ait 52W yüksek verisi yok.")
    else:
        hw1, hw2 = st.columns(2)
        hw1.metric("52H Yüksek Adayı", len(df_52w_f))
        hw2.metric("Trend Template'e Oranı", f"{len(df_52w)/len(df)*100:.1f}%")
        add_to_watchlist_button(df_52w_f, "tab2")

with tab3:
    st.subheader("📈 TPR Moving Up — Grade Yükselen Hisseler")
    st.caption("Önceki taramadan bu yana Grade yükselten hisseler (D→C, C→B, B→A)")
    if df_grade_up.empty:
        st.info("Karşılaştırma için önceki taramaya ihtiyaç var. Yarın bu liste dolacak.")
    else:
        g1, g2, g3 = st.columns(3)
        g1.metric("Toplam Yükselen", len(df_grade_up_f))
        g2.metric("+1 Grade", len(df_grade_up_f[df_grade_up_f["jump_size"] == 1]))
        g3.metric("+2 ve Üzeri", len(df_grade_up_f[df_grade_up_f["jump_size"] >= 2]))
        add_to_watchlist_button(df_grade_up_f, "tab3_grade_up")

with tab4:
    st.subheader("✅ Trend Template 8/8 — TAM UYUM")
    st.caption("Tüm 8 Minervini kuralı — MA200 slope dahil")
    show_all = st.checkbox("Tüm hisseleri göster (MA200 slope dahil geçemeyenler)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Finviz Geçen", len(df))
    m2.metric("8/8 Tam Uyum", len(df_pass_f))
    m3.metric("MA200 Slope Bekliyor", len(df_partial_f))
    m4.metric("Geçme Oranı", f"{len(df[df['passed']==1])/len(df)*100:.1f}%")
    if not df_pass_f.empty:
        add_to_watchlist_button(df_pass_f, "tab4_pass")
    else:
        st.warning("Bu tarihte 8/8 geçen hisse yok.")
    if show_all and not df_partial_f.empty:
        st.subheader("⚠️ MA200 Slope Geçemeyen (7/8)")
        add_to_watchlist_button(df_partial_f, "tab4_partial")

with tab5:
    st.subheader("📊 Sadece Temel Kriterler (Teknik Filtresiz)")
    st.caption("EPS Q/Q > %25 + Sales Q/Q > %25 + Fiyat > $10 + Hacim > 500K")
    if df_fund_only.empty:
        st.warning("Bu tarihe ait temel veri yok.")
    else:
        fo1, fo2 = st.columns(2)
        fo1.metric("Temel Kriter Geçen", len(df_fund_only_f))
        fo2.metric("Teknik Listeye Oranı", f"{len(df_fund_only)/len(df)*100:.1f}%")
        add_to_watchlist_button(df_fund_only_f, "tab5")

with tab6:
    st.subheader("⭐ Watch List")
    st.caption("Takip listenizdeki hisseler — tarihten bağımsız, veriler seçili tarihe göre")

    all_tickers = sorted(
        set(df["ticker"]) | set(df_52w["ticker"]) | set(df_fund_only["ticker"])
    )
    selected = st.multiselect(
        "Takip edilecek hisseler:",
        options=all_tickers,
        default=[t for t in wl_tickers if t in all_tickers],
    )

    if st.button("💾 Kaydet"):
        save_watchlist(selected)
        load_watchlist_data.clear()
        st.success(f"{len(selected)} hisse kaydedildi.")
        st.rerun()

    st.divider()

    if df_wl.empty:
        st.info("Watch list boş. Yukarıdan hisse ekle ve kaydet.")
    else:
        wl1, wl2 = st.columns(2)
        wl1.metric("Filtrelenmiş", len(df_wl_f))
        wl2.metric("Toplam Takip", len(df_wl))
        add_to_watchlist_button(df_wl_f, "tab6")
