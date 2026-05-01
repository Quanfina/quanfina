import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Kök dizindeki database.py dosyasına ulaşabilmek için
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import database as db

st.set_page_config(page_title="İstatistikler | Quanfina", layout="wide")

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
        conn = db.get_connection()
        df = pd.read_sql_query("SELECT symbol, strategy, r_multiple FROM trades WHERE status = 'Closed'", conn)
        conn.close()
        return df

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