import streamlit as st
import pandas as pd
import sqlite3
import os

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

if df.empty:
    st.warning("Bu tarihe ait veri yok.")
    st.stop()

# --- FİLTRE PANELİ ---
with st.expander("🔍 Filtreler", expanded=False):
    all_sectors = sorted(
        set(df["sector"].dropna()) |
        set(df_fund["sector"].dropna()) |
        set(df_fund_only["sector"].dropna())
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

col_rename = {
    "ticker": "TICKER", "company": "ŞİRKET", "sector": "SEKTÖR",
    "industry": "SEKTÖR ALTI", "price": "FİYAT", "change_pct": "DEĞİŞİM",
    "volume": "HACİM", "market_cap": "PİYASA DEĞERİ (M)",
    "ma200_slope": "MA200 SLOPE", "eps_qoq": "EPS Q/Q",
    "sales_qoq": "SALES Q/Q", "grade": "GRADE"
}

col_config = {
    "FİYAT": st.column_config.NumberColumn(format="$%.2f"),
    "MA200 SLOPE": st.column_config.NumberColumn(format="%.4f"),
    "HACİM": st.column_config.NumberColumn(format="%d"),
    "EPS Q/Q": st.column_config.NumberColumn(format="%.1f"),
    "SALES Q/Q": st.column_config.NumberColumn(format="%.1f"),
}

show_cols = ["ticker", "company", "sector", "industry", "price", "change_pct",
             "volume", "market_cap", "ma200_slope", "eps_qoq", "sales_qoq", "grade"]

# --- FUNDAMENTAL TABLO ---
st.divider()
st.subheader("⭐ Süper Performans Adayları (Teknik + EPS + Sales)")
st.caption("Trend Template 8/8 + EPS Q/Q > %25 + Sales Q/Q > %25")

if df_fund.empty:
    st.warning("Bu tarihe ait fundamental veri yok.")
else:
    f1, f2 = st.columns(2)
    f1.metric("Süper Performans Adayı", len(df_fund_f))
    f2.metric("Teknik Listeden Oranı", f"{len(df_fund)/len(df)*100:.1f}%")

    st.dataframe(
        df_fund_f[show_cols].rename(columns=col_rename),
        hide_index=True,
        use_container_width=True,
        column_config=col_config
    )

# Trend Template tablosu
st.divider()
show_all = st.checkbox("Tüm hisseleri göster (MA200 slope dahil geçemeyenler)")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Finviz Geçen", len(df))
m2.metric("8/8 Tam Uyum", len(df_pass_f))
m3.metric("MA200 Slope Bekliyor", len(df_partial_f))
m4.metric("Geçme Oranı", f"{len(df[df['passed']==1])/len(df)*100:.1f}%")
st.subheader("✅ Trend Template 8/8 — TAM UYUM")

if not df_pass_f.empty:
    st.dataframe(
        df_pass_f[show_cols].rename(columns=col_rename),
        hide_index=True,
        use_container_width=True,
        column_config=col_config
    )
else:
    st.warning("Bu tarihte 8/8 geçen hisse yok.")

# Kısmi geçenler
if show_all and not df_partial_f.empty:
    st.divider()
    st.subheader("⚠️ MA200 Slope Geçemeyen (7/8)")
    st.dataframe(
        df_partial_f[show_cols].rename(columns=col_rename),
        hide_index=True,
        use_container_width=True,
        column_config=col_config
    )

# --- SADECE TEMEL TABLO ---
st.divider()
st.subheader("📊 Sadece Temel Kriterler (Teknik Filtresiz)")
st.caption("EPS Q/Q > %25 + Sales Q/Q > %25 + Fiyat > $10 + Hacim > 500K")

if df_fund_only.empty:
    st.warning("Bu tarihe ait temel veri yok.")
else:
    fo1, fo2 = st.columns(2)
    fo1.metric("Temel Kriter Geçen", len(df_fund_only_f))
    fo2.metric("Teknik Listeye Oranı", f"{len(df_fund_only)/len(df)*100:.1f}%")

    st.dataframe(
        df_fund_only_f[show_cols].rename(columns=col_rename),
        hide_index=True,
        use_container_width=True,
        column_config=col_config
    )
