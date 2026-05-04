import streamlit as st
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from styles import apply_styles

st.set_page_config(page_title="Tum Sinyaller | Quanfina", layout="wide")
apply_styles()
