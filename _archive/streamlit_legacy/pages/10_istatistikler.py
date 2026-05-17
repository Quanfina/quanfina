import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Kök dizindeki database.py dosyasına ulaşabilmek için
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection import get_trades, get_setup_types, get_grade_categories, get_connection
from styles import apply_styles
from quanfina_math import (
    compute_rba_metrics,
    should_drop_setup,
    compute_grade_distribution,
    GRADE_CATEGORIES,
)

st.set_page_config(page_title="İstatistikler | Quanfina", layout="wide")
apply_styles()

st.title("📈 İstatistikler ve Performans")
st.write("Sistemin uzun vadeli beklentisi (Expectancy) ve R-Multiple analizleri.")

# Kullanıcının grafikleri test edebilmesi için geçici bir "Mock Data" (Test Verisi) butonu
test_modu = st.toggle("Sistemi Görmek İçin Test Verisi Yükle")

# Veritabanından veriyi çeken fonksiyon
def veri_getir():
    if test_modu:
        # Görsel test için sahte veriler
        data = {
            'symbol': ['AAPL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NFLX'],
            'strategy': ['Minervini VCP', 'Carr Setup #1', 'Minervini VCP', 'Diğer', 'Minervini Pivot', 'Carr Setup #1'],
            'r_multiple': [2.5, -1.0, 4.2, -0.5, 1.8, -1.0]
        }
        return pd.DataFrame(data)
    else:
        # Gerçek veritabanından sadece KAPANMIŞ işlemleri çekiyoruz
        return get_trades(status="Closed")[["symbol", "strategy", "r_multiple"]]

df_istatistik = veri_getir()

if df_istatistik.empty:
    st.info("ℹ️ İstatistiklerin hesaplanması için en az bir adet **kapanmış (Closed)** işleme ihtiyacımız var. İşlemleriniz kapandıkça grafikler burada otomatik olarak belirecektir.")
else:
    # --- 1. METRİKLER ---
    st.subheader("Özet Performans")
    col1, col2, col3, col4 = st.columns(4)
    
    toplam_islem = len(df_istatistik)
    kazanc_sayisi = len(df_istatistik[df_istatistik['r_multiple'] > 0])
    win_rate = (kazanc_sayisi / toplam_islem) * 100
    toplam_r = df_istatistik['r_multiple'].sum()
    ortalama_r = df_istatistik['r_multiple'].mean()
    
    col1.metric("Toplam Kapanan İşlem", f"{toplam_islem} Adet")
    col2.metric("Kazanma Oranı (Win Rate)", f"%{win_rate:.1f}")
    col3.metric("Toplam Kar/Zarar (R)", f"{toplam_r:.1f} R")
    col4.metric("Ortalama R-Çarpanı", f"{ortalama_r:.2f} R")
    
    st.divider()
    
    # --- 2. GRAFİKLER ---
    st.subheader("Görsel Analiz")
    c1, c2 = st.columns(2)
    
    with c1:
        # Plotly ile R-Multiple Dağılımı Çubuk Grafiği
        st.write("**İşlem Bazlı R-Multiple Çarpanları**")
        fig1 = px.bar(
            df_istatistik, 
            x='symbol', 
            y='r_multiple', 
            color='r_multiple',
            color_continuous_scale=px.colors.diverging.RdYlGn, # Kırmızıdan yeşile renk skalası
            labels={'symbol': 'Hisse Sembolü', 'r_multiple': 'Kazanılan/Kaybedilen R'}
        )
        st.plotly_chart(fig1, use_container_width=True)
        
    with c2:
        # Plotly ile Strateji Performansı Pasta Grafiği
        st.write("**Stratejilere Göre İşlem Dağılımı**")
        # Stratejilerin kaç kere kullanıldığını sayıyoruz
        strateji_sayilari = df_istatistik['strategy'].value_counts().reset_index()
        strateji_sayilari.columns = ['Strateji', 'İşlem Sayısı']
        
        fig2 = px.pie(
            strateji_sayilari, 
            values='İşlem Sayısı', 
            names='Strateji', 
            hole=0.4 # Ortası delik şık bir grafik yapar (Donut chart)
        )
        st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# RBA — Result Based Analysis (Sprint 4.7b)
# ============================================================
st.divider()
st.subheader("📊 RBA — Result Based Analysis (Adjusted Ratio)")
st.caption(
    "Mark Minervini metodolojisi: setup'ın istatistiksel edge'i. "
    "Min 30 trade gözlem gerekli (anlamlılık eşiği)."
)

closed_df_full = get_trades(status="Closed")

if test_modu:
    st.info("📊 RBA bölümü test modunda çalışmaz — gerçek kapalı trade verisi gerekir.")
elif "pnl_pct" not in closed_df_full.columns or closed_df_full.empty:
    st.info("📊 Henüz kapanmış trade yok. RBA hesabı için trade kapatın.")
else:
    closed_df = closed_df_full.dropna(subset=["pnl_pct"]).copy()

    if closed_df.empty:
        st.info("📊 Kapalı trade'ler var ama pnl_pct değerleri eksik.")
    else:
        closed_trades = closed_df[["pnl_pct"]].to_dict("records")
        rba = compute_rba_metrics(closed_trades)
        rec = should_drop_setup(rba)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Trade Sayısı", rba.num_trades)
        col2.metric("Win Rate", f"%{rba.win_rate*100:.0f}")
        col3.metric(
            "Adjusted Ratio",
            f"{rba.adjusted_ratio:.2f}" if rba.adjusted_ratio != float("inf") else "∞",
            help="(Win% × AvgGain) / (Loss% × |AvgLoss|). >1.0 = pozitif edge.",
        )
        col4.metric("Expectancy", f"%{rba.expectancy_pct:.2f}")
        col5.metric(
            "Avg Gain / Loss",
            f"%{rba.avg_gain_pct:.1f} / %{abs(rba.avg_loss_pct):.1f}",
        )

        if rec.severity == "OK":
            st.success(rec.message)
        elif rec.severity == "INFO":
            st.info(rec.message)
        elif rec.severity == "WARNING":
            st.warning(rec.message)
        elif rec.severity == "CRITICAL":
            st.error(rec.message)

        st.caption(
            f"En büyük kazanç: %{rba.largest_gain_pct:.1f}  •  "
            f"En büyük kayıp: %{rba.largest_loss_pct:.1f}  •  "
            f"İstatistiksel anlamlılık: "
            f"{'✅ Yeterli' if rba.is_statistically_significant else f'⏳ {30 - rba.num_trades} trade daha gerekli'}"
        )

        if "setup_type_id" in closed_df.columns and closed_df["setup_type_id"].notna().any():
            st.markdown("##### Setup Bazlı Kırılım")
            try:
                setup_pairs = get_setup_types()
                setup_map = {sid: sname for sid, sname in setup_pairs}
            except Exception:
                setup_map = {}

            setup_breakdown = []
            for setup_id, group in closed_df.groupby("setup_type_id"):
                if pd.isna(setup_id):
                    continue
                rba_sub = compute_rba_metrics(group[["pnl_pct"]].to_dict("records"))
                setup_breakdown.append({
                    "Setup": setup_map.get(int(setup_id), f"ID {int(setup_id)}"),
                    "Trade": rba_sub.num_trades,
                    "Win %": f"{rba_sub.win_rate*100:.0f}",
                    "AR": f"{rba_sub.adjusted_ratio:.2f}" if rba_sub.adjusted_ratio != float("inf") else "∞",
                    "Expectancy %": f"{rba_sub.expectancy_pct:.2f}",
                })

            if setup_breakdown:
                st.dataframe(pd.DataFrame(setup_breakdown), use_container_width=True, hide_index=True)
            else:
                st.caption("Setup bilgisi olan kapalı trade yok.")

# ── Sprint 4.7c.3d — TradeGrader Kategori Dağılımı (EK 10) ──────────
st.divider()
st.subheader("🎓 TradeGrader — Kategori Dağılımı")
st.caption(
    "Mark Minervini metodolojisi: 17 kategori (ENTRY×7 EXIT×5 LOSS×5). "
    "Her kategorinin Bundle hedef oranı vardır — "
    "ideal trader'ın trade'lerinin dağılımı."
)

try:
    _conn = get_connection()
    _cur = _conn.cursor()
    _cur.execute("""
        SELECT tgc.code
        FROM trade_legs tl
        JOIN trade_grade_categories tgc ON tgc.id = tl.grade_category_id
        WHERE tl.grade_category_id IS NOT NULL
        UNION ALL
        SELECT tgc.code
        FROM leg_exits le
        JOIN trade_grade_categories tgc ON tgc.id = le.grade_category_id
        WHERE le.grade_category_id IS NOT NULL
    """)
    _leg_rows = _cur.fetchall()
    _conn.close()
except Exception:
    _leg_rows = []

graded_legs = [{"grade_code": r[0]} for r in _leg_rows]

if not graded_legs:
    st.info(
        "📊 Henüz hiçbir trade leg'ine kategori atanmamış. "
        "TradeGrader manuel atama UI Sprint 4.7d'de gelecek. "
        "Aşağıda 17 kategorinin referans tablosu var."
    )
    cats = get_grade_categories()
    if cats:
        ref_rows = []
        for c in cats:
            t = c["target_pct"]
            ref_rows.append({
                "Kod":      c["code"],
                "Kategori": c["name"],
                "Tip":      c["leg_type"],
                "Hedef":    f"≥{t:.0f}%" if t and t > 0 else (f"≤{abs(t):.0f}%" if t else "—"),
            })
        st.dataframe(pd.DataFrame(ref_rows), use_container_width=True, hide_index=True)
else:
    distribution = compute_grade_distribution(graded_legs)
    if not distribution:
        st.info("📊 Grade verisi var ama tanımlı kategoriye uymuyor.")
    else:
        _status_emoji = {
            "WITHIN_TARGET": "🟢",
            "BELOW_TARGET":  "🔴",
            "ABOVE_TARGET":  "🔴",
            "INFO":          "ℹ️",
        }
        for leg_type, leg_label in [
            ("ENTRY", "🎯 ENTRY — Alım Dağılımı"),
            ("EXIT",  "💰 EXIT — Kâr Realize Dağılımı"),
            ("LOSS",  "🛑 LOSS — Zarar Kes Dağılımı"),
        ]:
            type_codes = [code for code, info in distribution.items() if info["leg_type"] == leg_type]
            type_count = sum(distribution[c]["count"] for c in type_codes)
            if type_count == 0:
                continue
            st.markdown(f"##### {leg_label} (n={type_count})")
            rows = []
            for code in type_codes:
                info = distribution[code]
                rows.append({
                    "":         _status_emoji.get(info["status"], "—"),
                    "Kod":      code,
                    "Kategori": info["name"],
                    "Sayı":     info["count"],
                    "Oran":     f"{info['actual_pct']:.1f}%",
                    "Hedef":    info["target"],
                    "Durum":    info["status"],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        below_n = sum(1 for info in distribution.values() if info["status"] == "BELOW_TARGET")
        above_n = sum(1 for info in distribution.values() if info["status"] == "ABOVE_TARGET")
        if below_n > 0 or above_n > 0:
            st.warning(
                f"⚠️ **{below_n}** kategori hedef altı • **{above_n}** kategori hedef üstü. "
                "Trade Journal'da geçmiş analizleri gözden geçir."
            )
        else:
            st.success("✅ Tüm kategoriler hedef dahilinde — disiplinli trader profili!")