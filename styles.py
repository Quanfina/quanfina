"""
Quanfina Tasarim Sistemi - Minervini Markets 360 esinli koyu tema.
PrimeVue Indigo brand + Tailwind Zinc surface paleti.
Tum sayfalarda apply_styles() cagrisiyla aktive edilir.
"""

import streamlit as st


# ============================================================
# TASARIM TOKENLARI (config.toml ile uyumlu)
# ============================================================

# Brand - Indigo (PrimeVue, Minervini'den birebir)
PRIMARY = "#818cf8"          # Indigo 400 - Dark mode aktif vurgu
PRIMARY_DARK = "#6366f1"     # Indigo 500 - Hover
PRIMARY_LIGHT = "#a5b4fc"    # Indigo 300 - Soft vurgu

# Surface - Zinc (PrimeVue)
BG = "#09090b"               # Zinc 950 - Sayfa BG
SURFACE = "#18181b"          # Zinc 900 - Kart, tablo BG
SURFACE_HOVER = "#27272a"    # Zinc 800 - Hover, secondary surface
BORDER = "#3f3f46"           # Zinc 700 - Border
DIVIDER = "#27272a"          # Tablo ic cizgileri

# Text
TEXT = "#ffffff"
TEXT_MUTED = "#80808e"       # Custom muted (CSS'ten)
TEXT_DIM = "#71717a"         # Zinc 500

# Semantic (canli pastel - CSS'ten dogrulanmis)
SUCCESS = "#58bd7d"
SUCCESS_LIGHT = "#9cd8b2"
SUCCESS_TEXT = "#78ca96"
DANGER = "#ff2424"
DANGER_LIGHT = "#ff5c5c"
WARNING = "#eab308"
ORANGE = "#fb923c"
PURPLE = "#c084fc"
GOLD = "#ffd700"

# Layout
RADIUS_SM = "6px"
RADIUS_MD = "8px"
RADIUS_LG = "12px"
RADIUS_PILL = "9999px"


# ============================================================
# CSS - Streamlit dark mode override + Minervini detaylari
# ============================================================
_CSS = f"""
<style>
/* Montserrat font - Google Fonts */
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&display=swap');

/* Tum metin Montserrat */
html, body, [class*="css"], .stMarkdown, .stText, button, input, textarea, select {{
    font-family: 'Montserrat', sans-serif !important;
}}

/* Basliklar */
h1, h2, h3, h4, h5, h6 {{
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em;
    color: {TEXT};
}}

/* Sayfa ana container */
.main, .block-container {{
    background-color: {BG};
}}

/* Sidebar (sol menu) */
section[data-testid="stSidebar"] {{
    background-color: {SURFACE} !important;
    border-right: 1px solid {BORDER};
}}

section[data-testid="stSidebar"] * {{
    color: {TEXT} !important;
}}

/* Sidebar nav linkler */
section[data-testid="stSidebar"] a {{
    color: {TEXT_MUTED} !important;
    border-radius: {RADIUS_SM};
    padding: 6px 10px;
    transition: all 0.15s ease;
}}

section[data-testid="stSidebar"] a:hover {{
    background-color: {SURFACE_HOVER};
    color: {TEXT} !important;
}}

/* Aktif sayfa - Indigo vurgu */
section[data-testid="stSidebar"] a[aria-current="page"] {{
    background-color: rgba(129, 140, 248, 0.15) !important;
    color: {PRIMARY} !important;
    font-weight: 600;
}}

/* Buton - dark moda uygun */
.stButton > button {{
    border-radius: {RADIUS_SM};
    font-weight: 600;
    background-color: {SURFACE_HOVER};
    color: {TEXT};
    border: 1px solid {BORDER};
    transition: all 0.15s ease;
}}

.stButton > button:hover {{
    background-color: {BORDER};
    border-color: {PRIMARY};
    transform: translateY(-1px);
}}

.stButton > button[kind="primary"] {{
    background-color: {PRIMARY};
    color: {BG};
    border: none;
}}

.stButton > button[kind="primary"]:hover {{
    background-color: {PRIMARY_DARK};
    color: white;
}}

/* Expander */
[data-testid="stExpander"] {{
    border: 1px solid {BORDER};
    border-radius: {RADIUS_SM};
    background-color: {SURFACE};
}}

[data-testid="stExpander"] summary,
.streamlit-expanderHeader {{
    background-color: {SURFACE} !important;
    color: {TEXT} !important;
    font-weight: 600;
    border-radius: {RADIUS_SM};
}}

[data-testid="stExpander"] summary:hover {{
    background-color: {SURFACE_HOVER} !important;
}}

/* Tab */
.stTabs [data-baseweb="tab-list"] {{
    gap: 4px;
    border-bottom: 1px solid {BORDER};
}}

.stTabs [data-baseweb="tab"] {{
    font-weight: 600;
    color: {TEXT_MUTED} !important;
}}

.stTabs [data-baseweb="tab"]:hover {{
    color: {TEXT} !important;
}}

.stTabs [aria-selected="true"] {{
    color: {PRIMARY} !important;
    border-bottom-color: {PRIMARY} !important;
}}

/* Metric kartlari */
[data-testid="stMetric"] {{
    background-color: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: {RADIUS_SM};
    padding: 16px;
}}

[data-testid="stMetricLabel"] {{
    color: {TEXT_MUTED} !important;
    font-size: 0.75rem !important;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

[data-testid="stMetricValue"] {{
    font-weight: 700;
    color: {TEXT} !important;
    font-size: 2rem !important;
}}

[data-testid="stMetricDelta"] {{
    font-weight: 600;
}}

/* DataFrame */
[data-testid="stDataFrame"] {{
    background-color: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: {RADIUS_SM};
}}

[data-testid="stDataFrame"] thead tr th {{
    background-color: {BG} !important;
    color: {TEXT_MUTED} !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    font-size: 0.75rem !important;
    letter-spacing: 0.05em;
    border-bottom: 1px solid {BORDER} !important;
}}

[data-testid="stDataFrame"] tbody tr td {{
    color: {TEXT} !important;
    border-color: {DIVIDER} !important;
}}

/* Selectbox, multiselect, date input */
[data-baseweb="select"] > div,
[data-baseweb="input"] > div,
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stDateInput > div > div {{
    background-color: {SURFACE} !important;
    border: 1px solid {BORDER} !important;
    border-radius: {RADIUS_SM} !important;
    color: {TEXT} !important;
}}

[data-baseweb="select"] > div:hover {{
    border-color: {PRIMARY} !important;
}}

/* Slider */
.stSlider [data-baseweb="slider"] {{
    color: {PRIMARY};
}}

/* Info/Success/Warning/Error kutulari */
.stAlert {{
    border-radius: {RADIUS_SM};
    border: 1px solid {BORDER};
    font-weight: 500;
}}

[data-testid="stNotificationContentInfo"] {{
    background-color: rgba(129, 140, 248, 0.1) !important;
    border-color: {PRIMARY} !important;
}}

[data-testid="stNotificationContentSuccess"] {{
    background-color: rgba(88, 189, 125, 0.1) !important;
    border-color: {SUCCESS} !important;
}}

[data-testid="stNotificationContentWarning"] {{
    background-color: rgba(234, 179, 8, 0.1) !important;
    border-color: {WARNING} !important;
}}

[data-testid="stNotificationContentError"] {{
    background-color: rgba(255, 36, 36, 0.1) !important;
    border-color: {DANGER} !important;
}}

/* Code blok */
code {{
    background-color: {SURFACE_HOVER};
    border: 1px solid {BORDER};
    border-radius: {RADIUS_SM};
    padding: 2px 6px;
    color: {PRIMARY_LIGHT};
    font-family: 'JetBrains Mono', 'Consolas', monospace;
}}

/* Linkler */
a {{
    color: {PRIMARY};
    text-decoration: none;
}}

a:hover {{
    color: {PRIMARY_LIGHT};
    text-decoration: underline;
}}

/* Header (Streamlit toolbar - Deploy butonu vs.) */
header[data-testid="stHeader"] {{
    background-color: transparent;
}}

/* Markdown text genel */
.stMarkdown, .stText {{
    color: {TEXT};
}}

.stMarkdown p {{
    color: {TEXT};
}}

/* Caption (kucuk gri yazilar) */
.stCaption, [data-testid="stCaptionContainer"] {{
    color: {TEXT_MUTED} !important;
}}
</style>
"""


def apply_styles():
    """Sayfanin basinda cagirin: Montserrat font + Minervini-tarzi koyu tema."""
    st.markdown(_CSS, unsafe_allow_html=True)
