import streamlit as st
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection import get_trades
from styles import apply_styles

st.set_page_config(page_title="Pozisyonlar | Quanfina", layout="wide")
apply_styles()

st.title("💼 Pozisyonlar")
st.write("Açık ve geçmiş işlemlerinizi buradan takip edebilirsiniz.")

tab1, tab2 = st.tabs(["🟢 Açık Pozisyonlar", "🔴 Geçmiş Pozisyonlar"])

with tab1:
    st.subheader("Canlı İşlemler")
    df_open = get_trades("Open")

    if df_open.empty:
        st.info("Şu an açık bir pozisyonunuz bulunmuyor. 'Yeni Pozisyon' sayfasından bir tane ekleyebilirsiniz.")
    else:
        st.dataframe(df_open, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Kapanmış İşlemler")
    df_closed = get_trades("Closed")

    if df_closed.empty:
        st.info("Henüz kapanmış bir işleminiz bulunmuyor.")
    else:
        st.dataframe(df_closed, use_container_width=True, hide_index=True)