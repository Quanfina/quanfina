import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import date

st.set_page_config(page_title="Minervini Trend Template", layout="wide")

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
        SELECT f.*, s.eps_qoq, s.sales_qoq, s.grade
        FROM minervini_fundamental_scans f
        LEFT JOIN minervini_scans s ON f.ticker = s.ticker AND f.scan_date = s.scan_date
        WHERE f.scan_date = ?
        ORDER BY f.ma200_slope DESC
    """, conn, params=(scan_date,))
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_fundamental_only(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM minervini_fundamental_only WHERE scan_date = ? ORDER BY market_cap DESC",
        conn, params=(scan_date,)
    )
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_52w_high(scan_date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM minervini_52w_high WHERE scan_date = ? ORDER BY ma200_slope DESC",
        conn, params=(scan_date,)
    )
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
            COALESCE(s.eps_qoq,     h.eps_qoq,     f.eps_qoq)    AS eps_qoq,
            COALESCE(s.sales_qoq,   h.sales_qoq,   f.sales_qoq)  AS sales_qoq,
            COALESCE(s.grade,       h.grade,       f.grade)       AS grade
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

def apply_filters(data, sektor, grade_list, siralama, arama):
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
    sort_map = {
        "MA200 Slope (azalan)": ("ma200_slope", False),
        "EPS Q/Q (azalan)":     ("eps_qoq",     False),
        "Sales Q/Q (azalan)":   ("sales_qoq",   False),
        "Hacim (azalan)":       ("volume",       False),
        "Ticker (A-Z)":         ("ticker",       True),
    }
    col, asc = sort_map[siralama]
    if col in d.columns:
        d = d.sort_values(col, ascending=asc, na_position="last")
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

if df.empty:
    st.warning("Bu tarihe ait veri yok.")
    st.stop()

# --- FİLTRE PANELİ ---
with st.expander("🔍 Filtreler", expanded=False):
    all_sectors = sorted(
        set(df["sector"].dropna()) |
        set(df_fund["sector"].dropna()) |
        set(df_fund_only["sector"].dropna()) |
        set(df_52w["sector"].dropna())
    )
    c1, c2, c3, c4 = st.columns(4)
    secim_sektor   = c1.selectbox("Sektör", ["Tümü"] + all_sectors)
    secim_grade    = c2.multiselect("Grade", ["A", "B", "C", "D"], default=["A", "B", "C", "D"])
    secim_siralama = c3.selectbox("Sıralama", [
        "MA200 Slope (azalan)",
        "EPS Q/Q (azalan)",
        "Sales Q/Q (azalan)",
        "Hacim (azalan)",
        "Ticker (A-Z)",
    ])
    secim_arama = c4.text_input("Hisse ara (TICKER / Şirket)")

# Filtrelenmiş varyantlar
df_pass_f      = apply_filters(df[df["passed"] == 1], secim_sektor, secim_grade, secim_siralama, secim_arama)
df_partial_f   = apply_filters(df[df["passed"] == 0], secim_sektor, secim_grade, secim_siralama, secim_arama)
df_fund_f      = apply_filters(df_fund,      secim_sektor, secim_grade, secim_siralama, secim_arama)
df_fund_only_f = apply_filters(df_fund_only, secim_sektor, secim_grade, secim_siralama, secim_arama)
df_52w_f       = apply_filters(df_52w,       secim_sektor, secim_grade, secim_siralama, secim_arama)
df_wl_f        = apply_filters(df_wl,        secim_sektor, secim_grade, secim_siralama, secim_arama)

st.caption(
    f"📊 Filtreli sonuçlar: "
    f"Süper Performans **{len(df_fund_f)}/{len(df_fund)}** · "
    f"Trend Template **{len(df_pass_f)}/{len(df[df['passed']==1])}** · "
    f"Sadece Temel **{len(df_fund_only_f)}/{len(df_fund_only)}** · "
    f"52H Yüksek **{len(df_52w_f)}/{len(df_52w)}** · "
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

def table_height(df):
    return min(len(df) * 35 + 50, 800)

def add_to_watchlist_button(df_filtered, key):
    event = st.dataframe(
        style_grade(add_link_columns(df_filtered)[show_cols].rename(columns=col_rename)),
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
    "ma200_slope": "MA200 SLOPE", "eps_qoq": "EPS Q/Q",
    "sales_qoq": "SALES Q/Q", "grade": "GRADE",
    "tv_url": "📊 TV", "fv_url": "📈 FV",
}

col_config = {
    "FİYAT": st.column_config.NumberColumn(format="$%.2f"),
    "MA200 SLOPE": st.column_config.NumberColumn(format="%.4f"),
    "HACİM": st.column_config.NumberColumn(format="%d"),
    "EPS Q/Q": st.column_config.NumberColumn(format="%.1f"),
    "SALES Q/Q": st.column_config.NumberColumn(format="%.1f"),
    "📊 TV": st.column_config.LinkColumn(display_text="🔗 Aç"),
    "📈 FV": st.column_config.LinkColumn(display_text="🔗 Aç"),
}

show_cols = ["ticker", "company", "sector", "industry", "price", "change_pct",
             "volume", "market_cap", "ma200_slope", "eps_qoq", "sales_qoq", "grade",
             "tv_url", "fv_url"]

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌟 Süper Performans",
    "🚀 52 Hafta Yüksek",
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
    st.subheader("✅ Trend Template 8/8 — TAM UYUM")
    st.caption("Tüm 8 Minervini kuralı — MA200 slope dahil")
    show_all = st.checkbox("Tüm hisseleri göster (MA200 slope dahil geçemeyenler)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Finviz Geçen", len(df))
    m2.metric("8/8 Tam Uyum", len(df_pass_f))
    m3.metric("MA200 Slope Bekliyor", len(df_partial_f))
    m4.metric("Geçme Oranı", f"{len(df[df['passed']==1])/len(df)*100:.1f}%")
    if not df_pass_f.empty:
        add_to_watchlist_button(df_pass_f, "tab3_pass")
    else:
        st.warning("Bu tarihte 8/8 geçen hisse yok.")
    if show_all and not df_partial_f.empty:
        st.subheader("⚠️ MA200 Slope Geçemeyen (7/8)")
        add_to_watchlist_button(df_partial_f, "tab3_partial")

with tab4:
    st.subheader("📊 Sadece Temel Kriterler (Teknik Filtresiz)")
    st.caption("EPS Q/Q > %25 + Sales Q/Q > %25 + Fiyat > $10 + Hacim > 500K")
    if df_fund_only.empty:
        st.warning("Bu tarihe ait temel veri yok.")
    else:
        fo1, fo2 = st.columns(2)
        fo1.metric("Temel Kriter Geçen", len(df_fund_only_f))
        fo2.metric("Teknik Listeye Oranı", f"{len(df_fund_only)/len(df)*100:.1f}%")
        add_to_watchlist_button(df_fund_only_f, "tab4")

with tab5:
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
        add_to_watchlist_button(df_wl_f, "tab5")
