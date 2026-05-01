import streamlit as st
import sqlite3
from datetime import date
import sys
import os

# Kök dizindeki database.py dosyasına ulaşabilmek için yolu ekliyoruz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import database as db

st.set_page_config(page_title="Yeni Pozisyon | Quanfina", layout="wide")

st.title("🎯 Yeni Pozisyon Ekle")
st.write("Bu sayfa **Van K. Tharp**'ın R-Multiple mantığına göre pozisyon büyüklüğünü otomatik hesaplar.")

# Ekranı iki sütuna bölüyoruz
col1, col2 = st.columns(2)

with col1:
    st.subheader("1. Portföy ve Risk")
    # Kullanıcıdan sayısal veri almak için number_input kullanırız
    account_size = st.number_input("Toplam Portföy Büyüklüğü ($)", min_value=100.0, value=10000.0, step=100.0)
    risk_pct = st.number_input("İşlem Başına Risk (%)", min_value=0.1, value=1.0, step=0.1)

with col2:
    st.subheader("2. İşlem Detayları")
    # Sembolü her zaman BÜYÜK harfe çevirerek (upper) kaydediyoruz
    symbol = st.text_input("Hisse Sembolü (Örn: AAPL)").upper()
    strategy = st.selectbox("Strateji", ["Minervini VCP", "Minervini Pivot", "Carr Setup #1", "Diğer"])
    entry_price = st.number_input("Giriş Fiyatı ($)", min_value=0.0, value=0.0, step=0.1)
    stop_loss = st.number_input("Stop Loss Fiyatı ($)", min_value=0.0, value=0.0, step=0.1)

st.divider() # Araya şık bir çizgi çeker

# === HESAPLAMA MANTIĞI ===
# Eğer fiyatlar girilmişse ve Long (Alım) işlemi mantığına uyuyorsa hesapla
if entry_price > 0 and stop_loss > 0 and entry_price > stop_loss:
    
    # 1. Tharp Hesaplamaları
    risk_per_share = entry_price - stop_loss
    total_risk_amount = account_size * (risk_pct / 100)
    quantity = int(total_risk_amount / risk_per_share) # Küsüratlı hisse alamayacağımız için tam sayıya (int) çeviriyoruz
    total_cost = quantity * entry_price
    
    # 2. Sonuçları Ekranda Gösterme
    st.subheader("📊 Pozisyon Büyüklüğü Tavsiyesi")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Alınacak Adet", f"{quantity} Adet")
    c2.metric("Toplam Sermaye İhtiyacı", f"${total_cost:,.2f}")
    c3.metric("1R (Toplam Riske Edilen)", f"${total_risk_amount:,.2f}")
    c4.metric("Hisse Başı Risk", f"${risk_per_share:,.2f}")
    
    st.info(f"💡 **Yorum:** Eğer fiyat {stop_loss} seviyesine düşer ve stop olursan, toplam portföyünün sadece %{risk_pct}'sini (yani ${total_risk_amount}) kaybetmiş olacaksın.")
    
    # 3. Disiplin ve Kayıt Bölümü
    st.write("### İşlem Onayı")
    plan_check = st.checkbox("Bu işlem Trading Planıma ve Strateji Kurallarıma %100 uyuyor.")
    
    if st.button("💾 Veritabanına Kaydet (Canlıya Al)"):
        if not plan_check:
            st.warning("Lütfen işlemi kaydetmeden önce Trading Planınıza uyduğunu onaylayın! (Disiplin kuralı)")
        elif symbol == "":
            st.error("Lütfen bir hisse sembolü girin.")
        else:
            # Veritabanına bağlanıp veriyi kaydediyoruz
            try:
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO trades (symbol, strategy, entry_date, entry_price, stop_loss, quantity, risk_amount, r_multiple, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, strategy, str(date.today()), entry_price, stop_loss, quantity, total_risk_amount, 1.0, 'Open'))
                conn.commit()
                conn.close()
                st.success(f"Tebrikler! {symbol} işlemi {quantity} adet olarak veritabanına kaydedildi.")
            except Exception as e:
                st.error(f"Kayıt sırasında bir hata oluştu: {e}")

elif entry_price > 0 and stop_loss >= entry_price:
    st.warning("Şu anda sadece **Long (Alım)** işlemleri desteklenmektedir. Stop Loss fiyatı, Giriş Fiyatından küçük olmalıdır.")