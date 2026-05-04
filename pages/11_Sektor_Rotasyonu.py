"""
Sektör Rotasyonu — 11 SPDR sektör ETF'i için RS Score sıralaması.
RS Score = (perf_1m × 0.4) + (perf_3m × 0.2) + (perf_6m × 0.2) + (perf_1y × 0.2)
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db_connection import get_engine
from styles import apply_styles, tag, colored_pct, card_title

st.set_page_config(page_title="Sektör Rotasyonu", page_icon="🔄", layout="wide")
apply_styles()


@st.cache_resource
def _get_engine():
    return get_engine()


@st.cache_data(ttl=300)
def load_dates():
    return pd.read_sql_query(
        "SELECT DISTINCT scan_date FROM sector_rotation ORDER BY scan_date DESC",
        _get_engine(),
    )


@st.cache_data(ttl=300)
def load_sectors(scan_date):
    return pd.read_sql_query(
        """
        SELECT rs_rank, ticker, sector_name,
               perf_1w, perf_1m, perf_3m, perf_6m, perf_1y, rs_score
        FROM sector_rotation
        WHERE scan_date = %s
        ORDER BY rs_rank
        """,
        _get_engine(),
        params=(scan_date,),
    )


# sector_rotation.sector_name → minervini_scans.sector eşleşme tablosu
_SECTOR_NAME_MAP = {
    "Financials":             "Financial",
    "Health Care":            "Healthcare",
    "Consumer Discretionary": "Consumer Cyclical",
    "Consumer Staples":       "Consumer Defensive",
    "Materials":              "Basic Materials",
}


@st.cache_data(ttl=300)
def load_top3_stocks(top3_sectors):
    # top3_sectors SPDR isimleri (sector_rotation) → Finviz isimlerine çevir
    finviz_sectors = [_SECTOR_NAME_MAP.get(s, s) for s in top3_sectors]
    engine = _get_engine()
    return pd.read_sql_query(
        """
        SELECT
            f.ticker, f.company, f.sector, f.industry,
            f.price, f.change_pct, f.volume, f.market_cap, f.pe,
            f.ma200_slope, f.high52, f.confirmations, f.violations,
            s.eps_qoq, s.sales_qoq, s.grade,
            COALESCE(f.rs_ibd,  s.rs_ibd)  AS rs_ibd,
            COALESCE(f.rs_12m,  s.rs_12m)  AS rs_12m
        FROM minervini_fundamental_scans f
        LEFT JOIN minervini_scans s
            ON f.ticker = s.ticker AND f.scan_date = s.scan_date
        WHERE f.scan_date = (SELECT MAX(scan_date) FROM minervini_fundamental_scans)
          AND f.sector = ANY(%s)
        ORDER BY COALESCE(f.rs_ibd, s.rs_ibd) DESC NULLS LAST
        """,
        engine,
        params=(finviz_sectors,),
    )


@st.cache_data(ttl=300)
def load_trend():
    return pd.read_sql_query(
        """
        SELECT scan_date, ticker, sector_name, rs_rank, rs_score
        FROM sector_rotation
        WHERE scan_date >= (
            SELECT MAX(scan_date) - INTERVAL '30 days'
            FROM sector_rotation
        )
        ORDER BY scan_date, rs_rank
        """,
        _get_engine(),
    )


# ── Sayfa başlığı ──────────────────────────────────────────────────────────────
st.title("🔄 Sektör Rotasyonu")
st.caption("11 SPDR Sektör ETF'i — Çoklu periyot ağırlıklı RS Score sıralaması")

dates_df = load_dates()
if dates_df.empty:
    st.warning("Henüz sektör tarama verisi yok. İlk gece taramasını bekle.")
    st.stop()

selected_date = st.selectbox(
    "Tarama tarihi:",
    dates_df["scan_date"].tolist(),
    format_func=lambda d: d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d),
)

df = load_sectors(selected_date)

# === MINERVINI TARZI OZET BANDI (3 KOLON) ===
if not df.empty:
    lider = df.iloc[0]
    zayif = df.iloc[-1]
    ort_rs = df["rs_score"].mean()
    sektor_sayisi = len(df)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(card_title("Lider Sektor"), unsafe_allow_html=True)
        st.markdown(
            f'<div style="display:flex; align-items:baseline; gap:8px; margin-bottom:8px;">'
            f'<span style="font-size:24px; font-weight:700;">{lider["ticker"]}</span>'
            f'{tag(lider["sector_name"], "info")}'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:14px;">RS: '
            f'<span style="font-size:20px; font-weight:700; color:#9cd8b2;">{lider["rs_score"]:.2f}</span> '
            f'· 1A {colored_pct(lider["perf_1m"], 1)}</div>',
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(card_title("Zayif Sektor"), unsafe_allow_html=True)
        st.markdown(
            f'<div style="display:flex; align-items:baseline; gap:8px; margin-bottom:8px;">'
            f'<span style="font-size:24px; font-weight:700;">{zayif["ticker"]}</span>'
            f'{tag(zayif["sector_name"], "danger")}'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:14px;">RS: '
            f'<span style="font-size:20px; font-weight:700; color:#ff6a6a;">{zayif["rs_score"]:.2f}</span> '
            f'· 1A {colored_pct(zayif["perf_1m"], 1)}</div>',
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(card_title("Ortalama RS"), unsafe_allow_html=True)
        st.markdown(
            f'<div style="font-size:24px; font-weight:700; margin-bottom:8px;">'
            f'{ort_rs:.2f}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:14px; color:#80808e;">'
            f'{sektor_sayisi} sektor analiz ediliyor</div>',
            unsafe_allow_html=True,
        )

    st.divider()

st.info(
    "📊 **RS Score formülü:** (1A × 0.4) + (3A × 0.2) + (6A × 0.2) + (1Y × 0.2). "
    "Haftalık performans (1H) sadece bilgi amaçlıdır, skora dahil değildir."
)

tab1, tab2, tab3 = st.tabs(["🏆 Sektör Sıralaması", "⭐ Top 3 Süper Performans", "📈 Rotasyon Trendi"])

# ── TAB 1 ─ Sektör Sıralaması ──────────────────────────────────────────────────
with tab1:
    st.subheader("RS Score Sıralaması")

    fig = px.bar(
        df.sort_values("rs_score"),
        x="rs_score",
        y="ticker",
        orientation="h",
        text="rs_score",
        color="rs_score",
        color_continuous_scale="RdYlGn",
        labels={"rs_score": "RS Score", "ticker": "ETF"},
        hover_data=["sector_name", "perf_1m", "perf_3m", "perf_6m", "perf_1y"],
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detaylı Tablo")
    display_df = df.copy()
    display_df.columns = [
        "Sıra", "Ticker", "Sektör",
        "1H %", "1A %", "3A %", "6A %", "1Y %", "RS Score",
    ]
    pct_cols = ["1H %", "1A %", "3A %", "6A %", "1Y %"]
    styled = (
        display_df.style
        .background_gradient(subset=pct_cols, cmap="RdYlGn", vmin=-20, vmax=20)
        .background_gradient(subset=["RS Score"], cmap="RdYlGn")
        .format({c: "{:.2f}" for c in pct_cols + ["RS Score"]})
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

# ── TAB 2 ─ Top 3 Sektör × Süper Performans hisseler ──────────────────────────
with tab2:
    st.subheader("⭐ Top 3 Sektördeki Süper Performans Hisseleri")
    st.caption(
        "RS Score'a göre en güçlü 3 sektörde yer alan ve Minervini Fundamental "
        "kriterlerini geçen hisseler. Sektör adı eşleşmesi Finviz'den gelen string'e "
        "göre yapılıyor — eşleşme olmazsa aşağıda uyarı görürsün."
    )

    top3 = df.head(3)
    cols = st.columns(3)
    for i, (_, row) in enumerate(top3.iterrows()):
        with cols[i]:
            st.metric(
                label=f"{int(row['rs_rank'])}. {row['ticker']} – {row['sector_name']}",
                value=f"RS {row['rs_score']:.2f}",
                delta=f"1A: {row['perf_1m']:.1f}%",
            )

    st.divider()

    top3_sectors = top3["sector_name"].tolist()
    try:
        super_df = load_top3_stocks(top3_sectors)
    except Exception as e:
        st.error(f"Sorgu hatası: {e}")
        super_df = pd.DataFrame()

    if super_df.empty:
        st.info(
            f"📭 {selected_date} tarihinde Top 3 sektörde Süper Performans hissesi bulunamadı. "
            "Sektör adı eşleşmesini kontrol et: sector_rotation.sector_name ile "
            "minervini_fundamental_scans.sector aynı string'i kullanıyor mu?"
        )
    else:
        st.success(f"✅ {len(super_df)} hisse Top 3 sektör + Süper Performans kesişiminde.")
        st.dataframe(super_df, use_container_width=True, hide_index=True)

# ── TAB 3 ─ Rotasyon Trendi ────────────────────────────────────────────────────
with tab3:
    st.subheader("📈 Sektör Rank Değişimi (Son 30 Gün)")
    st.caption(
        "Sektörlerin RS sıralamasının zamanla nasıl değiştiğini gösterir. "
        "Düşen rank = momentum kazanma."
    )

    trend_df = load_trend()

    if trend_df.empty or trend_df["scan_date"].nunique() < 2:
        st.info("📊 Trend grafiği için en az 2 günlük veri gerekiyor. Birkaç gün sonra tekrar bak.")
    else:
        fig2 = px.line(
            trend_df,
            x="scan_date",
            y="rs_rank",
            color="ticker",
            markers=True,
            hover_data=["sector_name", "rs_score"],
            labels={"scan_date": "Tarih", "rs_rank": "Sıralama (1 = en güçlü)"},
        )
        fig2.update_yaxes(autorange="reversed", dtick=1)
        fig2.update_layout(height=600, legend_title="ETF")
        st.plotly_chart(fig2, use_container_width=True)
