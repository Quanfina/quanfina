"""
3_Minervini.py - Mark Minervini SEPA Stratejisi (Yeni 4 Liste Mimarisi)

ADIM 4 Sprint 4.2: 4 dis tab iskelet
- Watch (100+ hisse)    - Genel bakis
- On Deck (30-40)       - On-eleme
- Focus (5-15)          - Detayli analiz
- Buy (3-5)             - Alim hazirligi

Her tab symbol_lists tablosundan oku (list_type kolonu ile filtreli).
Henuz veri yok - sonraki sprintlerde populate edilecek.

Eski 7+4 tab yapisi: pages/3_Minervini_old.py (rollback icin).
"""
import streamlit as st
import pandas as pd
from db_connection import get_connection
from _list_cols import get_columns_for_list, get_label, get_list_meta, LIST_TYPES


st.set_page_config(page_title="Minervini", page_icon="🎯", layout="wide")

st.title("🎯 Mark Minervini — 4 Liste Stratejisi")
st.caption("SEPA: Specific Entry Point Analysis | 4 kademeli liste yapisi")

USER_EMAIL = "ferit@quanfina.local"


@st.cache_data(ttl=60)
def get_user_id() -> int:
    """Ferit'in user_id'sini dondurur (cache'li)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE email = %s", (USER_EMAIL,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 1


@st.cache_data(ttl=60)
def load_list_data(list_code: str, user_id: int) -> pd.DataFrame:
    """symbol_lists tablosundan liste hisselerini cek + minervini_scans ile JOIN.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'
        user_id: Kullanici ID'si

    Returns:
        DataFrame: list metadata + scanner verisi (en son scan tarihi)
        Sutunlar:
        - symbol_lists: id, symbol, day_added, note, pivot_price, pullback_health, tt_score
        - minervini_scans (LEFT JOIN, en son scan): company, sector, price, change_pct,
          grade, rs_ibd, rs_12m, ma200_slope, pct_from_high, volume,
          eps_qoq, sales_qoq, market_cap, days_to_earnings, confirmations, violations
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(scan_date) FROM minervini_scans")
    last_scan_date = cur.fetchone()[0]

    cur.execute(
        "SELECT "
        "    sl.id, sl.symbol, sl.day_added, sl.note, "
        "    sl.pivot_price, sl.pullback_health, sl.tt_score, "
        "    s.company, s.sector, s.price, s.change_pct, "
        "    s.grade, s.rs_ibd, s.rs_12m, "
        "    s.ma200_slope, s.pct_from_high, s.volume, "
        "    s.eps_qoq, s.sales_qoq, s.market_cap, "
        "    s.days_to_earnings, s.confirmations, s.violations "
        "FROM symbol_lists sl "
        "LEFT JOIN minervini_scans s "
        "    ON s.ticker = sl.symbol AND s.scan_date = %s "
        "WHERE sl.user_id = %s AND sl.list_type = %s AND sl.strategy = 'minervini' "
        "ORDER BY sl.symbol",
        (last_scan_date, user_id, list_code)
    )
    rows = cur.fetchall()
    cols = [
        "id", "symbol", "day_added", "note",
        "pivot_price", "pullback_health", "tt_score",
        "company", "sector", "price", "change_pct",
        "grade", "rs_ibd", "rs_12m",
        "ma200_slope", "pct_from_high", "volume",
        "eps_qoq", "sales_qoq", "market_cap",
        "days_to_earnings", "confirmations", "violations",
    ]
    conn.close()
    return pd.DataFrame(rows, columns=cols)


def render_list_tab(list_code: str) -> None:
    """Bir liste tab'inin icerigini cizer.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'
    """
    meta = get_list_meta(list_code)
    expected_cols = get_columns_for_list(list_code)

    st.subheader(meta["label"])
    st.caption(meta["description"])

    user_id = get_user_id()
    df = load_list_data(list_code, user_id)

    col1, col2, col3 = st.columns(3)
    col1.metric("Hisse Sayisi", len(df))
    col2.metric("Hedef Sutun", meta["default_column_count"])
    col3.metric("Liste Sirasi", meta["sort_order"])

    if len(df) == 0:
        st.info(
            "**Bu liste henuz bos.**\n\n"
            "Sprint 4.{}'te bu liste populate edilecek. "
            "Mevcut Watch verisi icin sayfayi degistir: 'Minervini Old' "
            "(7+4 eski yapi paralel calisiyor).".format(meta["sort_order"] + 2)
        )
        with st.expander("Bu liste icin hedef sutun yapisi (Sprint 4.{})".format(meta["sort_order"] + 2)):
            st.write("**Sutun sayisi:** {} sutun".format(len(expected_cols)))
            st.write("**Sutunlar:**")
            label_data = [{"DB Kolon": c, "Display Label": get_label(c)} for c in expected_cols]
            st.dataframe(pd.DataFrame(label_data), use_container_width=True, hide_index=True)
        return

    st.dataframe(df, use_container_width=True, hide_index=True)


# 4 dis tab
tab_watch, tab_on_deck, tab_focus, tab_buy = st.tabs([
    "⭐ Watch (100+)",
    "🎯 On Deck (30-40)",
    "🔥 Focus (5-15)",
    "💰 Buy (3-5)",
])

with tab_watch:
    render_list_tab("watch")

with tab_on_deck:
    render_list_tab("on_deck")

with tab_focus:
    render_list_tab("focus")

with tab_buy:
    render_list_tab("buy")


st.divider()
st.caption(
    "Sprint 4.2 iskelet | symbol_lists tablosu | "
    "Eski 7+4 tab yapisi: 'Minervini Old' sayfasi"
)
