import streamlit as st
from datetime import date
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection import get_journal, insert_journal
from styles import apply_styles

st.set_page_config(page_title="Trade Journal | Quanfina", layout="wide")
apply_styles()

st.title("📓 Trade Journal")
st.write("Trader'ın aynası. Kararlarını, hatalarını ve derslerini buraya kaydet.")

# 5 Ana Bölüm için sekmelerimizi oluşturuyoruz
tab_gunluk, tab_trade, tab_plan, tab_hata, tab_aylik = st.tabs([
    "📝 Günlük Not", 
    "🎯 Trade Notları", 
    "📜 Trading Plan", 
    "🚨 Hata Kataloğu", 
    "📅 Aylık Değerlendirme"
])

# --- 1. GÜNLÜK NOT BÖLÜMÜ ---
with tab_gunluk:
    st.subheader("Piyasa ve Psikoloji Notları")
    st.write("Bugün piyasa nasıl hissettiriyor? Kendi psikolojin nasıl? Özgürce yaz.")
    
    # Kullanıcıdan not alıyoruz
    daily_note = st.text_area("Bugünün Notu:", height=150)
    
    if st.button("💾 Günlük Notu Kaydet"):
        if daily_note.strip() == "":
            st.warning("Lütfen kaydetmeden önce bir şeyler yazın.")
        else:
            try:
                insert_journal(date.today(), "Daily", daily_note)
                st.success("Günlük notunuz başarıyla kaydedildi!")
            except Exception as e:
                st.error(f"Kayıt hatası: {e}")
                
    st.divider()
    st.markdown("### Geçmiş Günlük Notlar")
    # Kayıtlı notları veritabanından çekip gösterelim
    try:
        df_raw = get_journal(category="Daily")
        df_notes = df_raw[["date", "content"]].rename(columns={"date": "Tarih", "content": "Not_İçeriği"})

        if df_notes.empty:
            st.info("Henüz geçmiş bir not bulunmuyor.")
        else:
            st.dataframe(df_notes, use_container_width=True, hide_index=True)
    except Exception as e:
        st.write(f"Notlar yüklenirken bir sorun oluştu: {e}")

# --- DİĞER BÖLÜMLER (Şimdilik İskelet) ---
with tab_trade:
    st.subheader("Trade Bazlı Notlar")
    st.info("Burada ileride veritabanındaki açık/geçmiş işlemlerini seçip onlara özel giriş/çıkış sebepleri ve ekran görüntüleri ekleyebileceksin.")

with tab_plan:
    st.subheader("Trading Plan ve Kurallar")
    st.info("Sistemin değişmez kurallarını buraya listeleyeceğiz. Her işleme girmeden önce buradan teyit edeceksin.")

with tab_hata:
    st.subheader("Hata Kataloğu")
    st.info("Tekrar eden hatalarını (örn: FOMO, Erken Çıkış, Stop Kaydırma) buradan etiketleyip istatistiklerini tutacağız.")

with tab_aylik:
    st.subheader("Aylık Öz-Değerlendirme")
    st.info("Her ayın sonunda R-Multiple performansını ve psikolojik gelişimini burada değerlendireceksin.")