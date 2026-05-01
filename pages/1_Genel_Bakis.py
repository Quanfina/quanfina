import streamlit as st
import pandas as pd
import sys
import os

# Kök dizindeki database.py dosyasına ulaşabilmek için yol ayarı
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import database as db

# Sayfa ayarları (Bu hep en üstte olmalıdır)
st.set_page_config(page_title="Genel Bakış | Quanfina", page_icon="📊", layout="wide")

st.title("📊 Genel Bakış")
st.write("Quanfina sistemine hoş geldin. İşte portföyünün ve piyasanın anlık özeti.")

# --- 1. SATIR: ÖZET METRİKLER (Dashboard) ---
st.subheader("Gösterge Paneli")
col1, col2, col3, col4 = st.columns(4)

# Veritabanından açık işlem sayısını ve toplam riski çekelim
try:
    conn = db.get_connection()
    df_open = pd.read_sql_query("SELECT * FROM trades WHERE status = 'Open'", conn)
    conn.close()
    
    acik_islem_sayisi = len(df_open)
    toplam_risk = df_open['risk_amount'].sum() if not df_open.empty else 0.0
except Exception as e:
    acik_islem_sayisi = 0
    toplam_risk = 0.0
    st.error(f"Veri çekilirken hata oluştu: {e}")

with col1:
    # İleride 2_Piyasa_Durumu sayfasından otomatik gelecek, şimdilik statik
    st.metric(label="Piyasa Rejimi (Mini)", value="🟢 Risk-On", delta="Yükseliş Trendi")
    st.caption("*(Rejim skoru ileride entegre edilecek)*")

with col2:
    # Veritabanından canlı okunan değer
    st.metric(label="Açık Pozisyon Sayısı", value=f"{acik_islem_sayisi} Adet")

with col3:
    # Veritabanından canlı okunan değer (Toplam Risk)
    st.metric(label="Toplam Açık Risk ($)", value=f"${toplam_risk:,.2f}", delta="Isı Normal", delta_color="off")

with col4:
    # İleride İstatistikler sayfasından hesaplanacak
    st.metric(label="Bu Ayki Performans", value="+2.5 R", delta="Hedefte")
    st.caption("*(Örnek/Hedef veri)*")

st.divider()

# --- 2. SATIR: AÇIK POZİSYONLAR MİNİ TABLOSU ---
st.subheader("Canlı Pozisyonlar Özeti")

if acik_islem_sayisi == 0:
    st.info("Şu an açık bir pozisyonun bulunmuyor. Yeni bir fırsat mı var?")
else:
    # Sadece dashboard'a uygun, en önemli sütunları filtreleyip gösterelim
    df_mini = df_open[['symbol', 'strategy', 'entry_price', 'quantity', 'risk_amount']]
    
    # Sütun isimlerini daha şık görünmesi için Türkçeleştirelim
    df_mini.columns = ['Sembol', 'Strateji', 'Giriş Fiyatı ($)', 'Adet', 'Riske Edilen Tutar ($)']
    
    st.dataframe(df_mini, use_container_width=True, hide_index=True)

st.divider()

# --- 3. SATIR: HIZLI AKSİYON BUTONLARI ---
st.subheader("⚡ Hızlı Aksiyonlar")
c1, c2, c3 = st.columns(3)

with c1:
    # st.switch_page komutu ile Streamlit içinde sayfalar arası hızlı geçiş yapabiliriz
    if st.button("➕ Yeni Pozisyon Ekle", use_container_width=True):
        st.switch_page("pages/6_Yeni_Pozisyon.py")
with c2:
    if st.button("📓 Günlüğe Not Yaz", use_container_width=True):
        st.switch_page("pages/9_Trade_Journal.py")