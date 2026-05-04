import streamlit as st
from styles import apply_styles

# Sayfa ayarlarını yapılandırıyoruz (Tarayıcı sekmesindeki isim ve geniş ekran)
st.set_page_config(page_title="Quanfina", page_icon="📈", layout="wide")
apply_styles()

# Ekrana ana başlığımızı yazdırıyoruz
st.title("Quanfina'ya Hoş Geldin! 🚀")

# Kullanıcıya bilgi mesajı
st.write("Sistem başarıyla kuruldu. Altyapı çalışmaya hazır.")