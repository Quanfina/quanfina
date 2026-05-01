import streamlit as st
import pandas as pd
import sys
import os

# Kök dizindeki database.py dosyasına ulaşabilmek için yolu ekliyoruz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import database as db

st.set_page_config(page_title="Pozisyonlar | Quanfina", layout="wide")

st.title("💼 Pozisyonlar")
st.write("Açık ve geçmiş işlemlerinizi buradan takip edebilirsiniz.")

# Streamlit ile iki ayrı sekme (Tab) oluşturuyoruz
tab1, tab2 = st.tabs(["🟢 Açık Pozisyonlar", "🔴 Geçmiş Pozisyonlar"])

def load_trades(status):
    """Veritabanından belirli statüdeki (Open/Closed) işlemleri çeker."""
    conn = db.get_connection()
    # Pandas kullanarak SQL sorgusunun sonucunu doğrudan bir veri tablosuna (DataFrame) çeviriyoruz
    query = f"SELECT id, symbol, strategy, entry_date, entry_price, stop_loss, quantity, risk_amount, r_multiple FROM trades WHERE status = '{status}'"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

with tab1:
    st.subheader("Canlı İşlemler")
    df_open = load_trades('Open')
    
    if df_open.empty:
        st.info("Şu an açık bir pozisyonunuz bulunmuyor. 'Yeni Pozisyon' sayfasından bir tane ekleyebilirsiniz.")
    else:
        # Tabloyu tüm genişliği kaplayacak şekilde ekrana basıyoruz
        st.dataframe(df_open, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Kapanmış İşlemler")
    df_closed = load_trades('Closed')
    
    if df_closed.empty:
        st.info("Henüz kapanmış bir işleminiz bulunmuyor.")
    else:
        st.dataframe(df_closed, use_container_width=True, hide_index=True)