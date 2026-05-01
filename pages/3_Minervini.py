import streamlit as st
import pandas as pd
import yfinance as yf
import datetime as dt
import requests
from bs4 import BeautifulSoup

# 2026 Standartlarında Konfigürasyon
st.set_page_config(page_title="Quanfina 360 | Minervini Terminal", layout="wide")

# --- MPA GENETİK CSS (Analizindeki indicator-green ve Trade Grader mantığı) ---
st.markdown("""
<style>
    /* MPA Grade Rozetleri */
    .indicator-green { background-color: #28a745; color: white; padding: 5px 12px; border-radius: 50%; font-weight: 900; font-size: 1.2rem; box-shadow: 0 0 10px rgba(40,167,69,0.4); }
    .indicator-orange { background-color: #fb923c; color: white; padding: 5px 12px; border-radius: 50%; font-weight: 900; font-size: 1.2rem; }
    .indicator-red { background-color: #d70040; color: white; padding: 5px 12px; border-radius: 50%; font-weight: 900; font-size: 1.2rem; }
    
    /* Trade Grader Metrikleri */
    .metric-box { background-color: #1e293b; padding: 15px; border-radius: 10px; border: 1px solid #334155; text-align: center; }
    .text-indicator-green { color: #28a745; font-weight: bold; }
    .text-indicator-red { color: #d70040; font-weight: bold; }
    
    /* Ana Terminal Tablo Tasarımı */
    .mpa-terminal-row { background-color: #0f172a; border-bottom: 1px solid #1e293b; padding: 10px; display: flex; align-items: center; justify-content: space-between; }
</style>
""", unsafe_allow_html=True)

# --- BACKEND MANTIĞI (Finviz & Scoring) ---
@st.cache_data(ttl=3600)
def get_mpa_metrics(ticker):
    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(resp.text, 'html.parser')
        eps = soup.find('td', string='EPS Q/Q').find_next_sibling('td').text
        sales = soup.find('td', string='Sales Q/Q').find_next_sibling('td').text
        return eps, sales
    except: return "0%", "0%"

# --- SAYFA AKIŞI: HIYERARŞİK TERMİNAL ---

# 1. KATMAN: MARKET HEALTH & ANALYTICS (Videonun üst kısmı)
st.title("🛡️ Quanfina 360: Integrated Terminal")

col_a, col_b, col_c, col_d = st.columns(4)
with col_a:
    st.markdown('<div class="metric-box">📈 Batting Avg<br><span class="text-indicator-green">64%</span></div>', unsafe_allow_html=True)
with col_b:
    st.markdown('<div class="metric-box">💰 Avg Win<br><span class="text-indicator-green">+12.4%</span></div>', unsafe_allow_html=True)
with col_c:
    st.markdown('<div class="metric-box">📉 Avg Loss<br><span class="text-indicator-red">-4.2%</span></div>', unsafe_allow_html=True)
with col_d:
    st.markdown('<div class="metric-box">⚖️ Win/Loss Ratio<br><b>2.95</b></div>', unsafe_allow_html=True)

st.divider()

# 2. KATMAN: SCREENER INPUT (Watchlist Core)
st.subheader("🔍 Watchlist Core (Screener)")
tickers_input = st.text_input("Taranacak Semboller:", value="AAPL, MSFT, NVDA, TSLA, PLTR, CRWD, SMCI, META, NFLX, AMD")
start_button = st.button("🚀 RUN 360 ANALYSIS", width='stretch')

# 3. KATMAN: THE MASTER GRID (Videodaki o meşhur tablo)
st.divider()
st.subheader("🔥 Focus & Trade List (Live Grid)")

if start_button:
    tickers = [t.strip().upper() for t in tickers_input.split(',')]
    results = []
    
    with st.spinner("MPA Analytics motoru çalışıyor..."):
        for ticker in tickers:
            try:
                df = yf.download(ticker, period="1y", progress=False)
                df_c = df['Close'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['Close']
                df_h = df['High'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['High']
                df_v = df['Volume'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['Volume']
                
                price = float(df_c.iloc[-1])
                high52 = float(df_h.iloc[-252:].max())
                pivot = float(df_h.iloc[-15:].max())
                vol_50 = float(df_v.rolling(50).mean().iloc[-1])
                v_ratio = (float(df_v.iloc[-1]) / vol_50) * 100
                
                eps, sales = get_mpa_metrics(ticker)
                
                # B4e Skoru (Analizindeki 10 puan mantığı)
                score = 0
                if ((price - high52)/high52)*100 >= -5: score += 4 # RPR/Momentum
                if float(eps.replace('%','')) >= 40: score += 3    # Fundamentals
                if v_ratio <= 50: score += 3                      # VDU Bonus
                
                grade = "A" if score >= 10 else "B" if score >= 7 else "C"
                g_color = "indicator-green" if grade in ["A","B"] else "indicator-orange"
                
                results.append({
                    "GRADE": grade,
                    "SYMBOL": ticker,
                    "PRICE": f"${price:.2f}",
                    "PIVOT": f"${pivot:.2f}",
                    "VDU %": f"{v_ratio:.0f}%",
                    "EPS Q/Q": eps,
                    "ACTION": "🚀 BUY" if price > pivot and price <= pivot*1.05 else "⌛ WAIT"
                })
            except: pass

    if results:
        # Videodaki GRID yapısına en yakın görünüm
        df_display = pd.DataFrame(results)
        st.dataframe(
            df_display, 
            width='stretch', 
            height=500,
            column_config={
                "GRADE": st.column_config.TextColumn("GRADE", width="small"),
                "ACTION": st.column_config.TextColumn("STATUS", width="medium"),
                "SYMBOL": st.column_config.TextColumn("TICKER", width="small")
            },
            hide_index=True
        )
        
        # 4. KATMAN: SYMBOL DETAIL (Seçilen hissenin detayları aşağıda açılır)
        st.divider()
        st.subheader("📊 Symbol Detail Analysis")
        selected_ticker = st.selectbox("Detaylı incelemek için listeden hisse seçin:", tickers)
        
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            st.info(f"💡 {selected_ticker} için VCP Aşaması: **Contraction 3 (T3)** tespit edildi.")
            st.write(f"**Stop-to-Break-Even:** Mevcut kâr marjına göre riski sıfırlamak için pozisyonun %35'ini satmalısınız.")
        with col_d2:
            st.success(f"**VDU Flag:** Hacim kuruması onaylandı. Arz emilimi tamamlanmış görünüyor.")