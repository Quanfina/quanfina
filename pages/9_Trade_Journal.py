import streamlit as st
from datetime import date
import sys
import os
import pandas as pd

# Kök dizindeki database.py dosyasına ulaşabilmek için
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import database as db

st.set_page_config(page_title="Trade Journal | Quanfina", layout="wide")

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
                conn = db.get_connection()
                cursor = conn.cursor()
                # 'journal' tablosuna 'Daily' kategorisiyle kaydediyoruz
                cursor.execute('''
                    INSERT INTO journal (date, category, content)
                    VALUES (?, ?, ?)
                ''', (str(date.today()), 'Daily', daily_note))
                conn.commit()
                conn.close()
                st.success("Günlük notunuz başarıyla kaydedildi!")
            except Exception as e:
                st.error(f"Kayıt hatası: {e}")
                
    st.divider()
    st.markdown("### Geçmiş Günlük Notlar")
    # Kayıtlı notları veritabanından çekip gösterelim
    try:
        conn = db.get_connection()
        df_notes = pd.read_sql_query("SELECT date as Tarih, content as Not_İçeriği FROM journal WHERE category='Daily' ORDER BY id DESC", conn)
        conn.close()
        
        if df_notes.empty:
            st.info("Henüz geçmiş bir not bulunmuyor.")
        else:
            st.dataframe(df_notes, use_container_width=True, hide_index=True)
    except:
        st.write("Notlar yüklenirken bir sorun oluştu.")

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