import streamlit as st
from styles import apply_styles, status_dot, tag

# Sayfa ayarlarını yapılandırıyoruz (Tarayıcı sekmesindeki isim ve geniş ekran)
st.set_page_config(page_title="Quanfina", page_icon="📈", layout="wide")
apply_styles()

# Ekrana ana başlığımızı yazdırıyoruz
st.title("Quanfina'ya Hoş Geldin! 🚀")

# Kullanıcıya bilgi mesajı
st.write("Sistem başarıyla kuruldu. Altyapı çalışmaya hazır.")

st.markdown("---")

col1, col2, col3 = st.columns([2, 2, 2])

with col1:
    st.markdown(status_dot("Cloud SQL: Connected", "online"), unsafe_allow_html=True)

with col2:
    st.markdown(status_dot("Scanner: Active", "online"), unsafe_allow_html=True)

with col3:
    st.markdown(
        f'<div style="text-align:right;">{tag("v1.0 · Sektor Rotasyonu", "info")}</div>',
        unsafe_allow_html=True,
    )