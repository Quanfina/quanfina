import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Piyasa Durumu | Quanfina", layout="wide")

st.title("🧭 Piyasa Durumu ve Rejim")
st.write("İşlemlere girmeden önce 'Hava Durumu' kontrolü. Risk-On mu, Risk-Off mu?")

# --- 1. ANA REJİM BELİRLEYİCİ ---
st.subheader("Ana Piyasa Rejimi")
col1, col2 = st.columns([1, 3])

with col1:
    # Kullanıcının manuel olarak rejimi seçebileceği bir kutu
    rejim = st.selectbox(
        "Mevcut Rejim",
        ["🟢 Güçlü Yükseliş (Risk-On)", "🟡 Baskı Altında Yükseliş", "🟠 Düzeltme (Karışık)", "🔴 Düşüş Trendi (Risk-Off)"]
    )

with col2:
    if "🟢" in rejim:
        st.success("**Aksiyon Planı:** Agresif alım yapılabilir. Odak listesindeki (Focus List) kırılımlar tam pozisyon (1R) ile alınır.")
    elif "🟡" in rejim:
        st.warning("**Aksiyon Planı:** Seçici alım. Pozisyon büyüklükleri küçültülmeli (0.5R). Erken kar alma düşünülebilir.")
    elif "🟠" in rejim:
        st.warning("**Aksiyon Planı:** Yeni alım yapmak risklidir. Mevcut pozisyonların stopları sıkılaştırılır.")
    else:
        st.error("**Aksiyon Planı:** Nakitte kal (Cash is King). Yeni uzun (long) pozisyon açılmaz. Belki short setup'lar aranır.")

st.divider()

# --- 2. ENDEKS SAĞLIĞI VE DAĞITIM GÜNLERİ ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("Endeksler ve 30-Haftalık MA")
    st.write("Fiyat 30 haftalık hareketli ortalamanın üstünde mi?")
    
    # Mock (Örnek) Data
    df_endeks = pd.DataFrame({
        'Endeks': ['S&P 500 (SPY)', 'Nasdaq (QQQ)', 'Russell 2000 (IWM)'],
        'Durum': ['🟢 Üstünde', '🟢 Üstünde', '🔴 Altında'],
        'Trend': ['Yükseliş', 'Güçlü Yükseliş', 'Yatay/Düşüş']
    })
    st.dataframe(df_endeks, use_container_width=True, hide_index=True)

with c2:
    st.subheader("Dağıtım Günleri (Distribution Days)")
    st.write("Kurumsal satış baskısının takibi (Son 25 işlem günü)")
    
    col_d1, col_d2 = st.columns(2)
    col_d1.metric("SPY Dağıtım Günü", "3 Gün", "Normal Seviye", delta_color="off")
    col_d2.metric("QQQ Dağıtım Günü", "5 Gün", "Dikkat!", delta_color="inverse")
    
    st.info("💡 FTD (Follow-Through Day): Beklenmiyor (Mevcut trend zaten yukarı).")

st.divider()

# --- 3. SEKTÖR HEATMAP VE RİSK GÖSTERGELERİ ---
st.subheader("Sektör Rotasyonu ve Risk Göstergeleri")
c3, c4 = st.columns(2)

with c3:
    st.write("**Örnek Sektör Isı Haritası (Heatmap)**")
    # Plotly Treemap (Isı Haritası) için örnek veri
    df_sektor = pd.DataFrame({
        'Sektör': ['Teknoloji', 'Finans', 'Sağlık', 'Enerji', 'Sanayi', 'Tüketici', 'Gayrimenkul'],
        'Performans': [2.5, 1.2, -0.5, -1.8, 0.8, -0.2, -1.0],
        'Büyüklük': [40, 25, 20, 15, 15, 10, 5]
    })
    
    fig = px.treemap(
        df_sektor, 
        path=['Sektör'], 
        values='Büyüklük',
        color='Performans',
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=0
    )
    fig.update_layout(margin=dict(t=0, l=0, r=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.write("**Risk İndikatörleri (Korku ve Güvenli Limanlar)**")
    
    # 3 metrik yan yana
    m1, m2, m3 = st.columns(3)
    m1.metric("VIX (Korku)", "14.50", "-1.2", delta_color="inverse") # VIX düşmesi iyidir (inverse)
    m2.metric("TLT (Tahvil)", "95.20", "+0.4")
    m3.metric("GLD (Altın)", "215.30", "+1.1")
    
    st.write("") # Boşluk
    st.caption("Piyasa radarı: VIX 20'nin altında oldukça piyasa sakindir. TLT ve GLD'deki ani zıplamalar 'Risk-Off' sinyali verebilir.")