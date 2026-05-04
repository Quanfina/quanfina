"""
Sprint 3C-2 — Pozisyonlar Sayfası (Multi-Action)
Açık ve Kapalı pozisyonları yönetir.
Aksiyonlar: ✏️ Düzenle | 🚪 Kapat | 🗑️ Sil
"""
import streamlit as st
import pandas as pd
from datetime import date, datetime, time
from db_connection import (
    get_trades, get_portfolio,
    update_trade, close_trade, delete_trade,
)
from styles import (
    apply_styles, chip_long, chip_short, colored_pct, tag,
    card_title, status_dot,
    PRIMARY, SUCCESS, DANGER, TEXT, TEXT_MUTED, SURFACE, BORDER, WARNING,
)

# ──────────────────────────────────────────────────────────
# Page Config + Styles
# ──────────────────────────────────────────────────────────
st.set_page_config(page_title="Pozisyonlar — Quanfina", layout="wide")
apply_styles()

# ──────────────────────────────────────────────────────────
# Portfolio Şeridi (6_Yeni_Pozisyon ile birebir)
# ──────────────────────────────────────────────────────────
portfolio = get_portfolio(1)
if portfolio:
    starting_value = float(portfolio.get('starting_value', 10000))
    current_value = float(portfolio.get('current_value', starting_value))
    ytd_pct = ((current_value - starting_value) / starting_value * 100) if starting_value > 0 else 0
    ytd_color = SUCCESS if ytd_pct >= 0 else DANGER
    ytd_sign = "+" if ytd_pct >= 0 else ""

    st.markdown(f"""
    <div style="
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 14px 22px;
        margin-bottom: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    ">
        <div style="display: flex; align-items: center; gap: 18px;">
            <span style="color: {TEXT_MUTED}; font-size: 13px;">💼 Portföy</span>
            <span style="color: {TEXT}; font-size: 20px; font-weight: 700;">${current_value:,.2f}</span>
            <span style="color: {ytd_color}; font-size: 14px; font-weight: 600;">YTD {ytd_sign}{ytd_pct:.2f}%</span>
        </div>
        <span style="color: {TEXT_MUTED}; font-size: 12px;">Başlangıç: ${starting_value:,.2f}</span>
    </div>
    """, unsafe_allow_html=True)

# Sayfa başlığı
st.markdown(card_title("📊 Pozisyonlar", "Açık ve geçmiş tüm işlemleriniz"), unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────
# Session State Initialization
# ──────────────────────────────────────────────────────────
for key in ('edit_trade_id', 'close_trade_id', 'delete_confirm_id'):
    if key not in st.session_state:
        st.session_state[key] = None

def _clear_action_state():
    """Tüm aksiyon state'lerini sıfırla."""
    st.session_state['edit_trade_id'] = None
    st.session_state['close_trade_id'] = None
    st.session_state['delete_confirm_id'] = None

def _set_action(action: str, trade_id: int):
    """Bir aksiyon set ederken diğerlerini sıfırla."""
    _clear_action_state()
    st.session_state[f'{action}_trade_id' if action != 'delete' else 'delete_confirm_id'] = trade_id

# ──────────────────────────────────────────────────────────
# Filter Bar
# ──────────────────────────────────────────────────────────
st.markdown("##### 🔍 Filtreler")
fc1, fc2, fc3, fc4, fc5 = st.columns([2, 1.5, 1.5, 1.5, 1])

with fc1:
    filter_symbol = st.text_input("Sembol", placeholder="örn. AAPL", key="filter_symbol").strip().upper()

with fc2:
    filter_direction = st.selectbox("Yön", ["Tümü", "Long", "Short"], key="filter_direction")

with fc3:
    filter_date_from = st.date_input("Başlangıç tarihi", value=None, key="filter_date_from")

with fc4:
    filter_date_to = st.date_input("Bitiş tarihi", value=None, key="filter_date_to")

with fc5:
    st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)  # label hizası
    if st.button("🔄 Sıfırla", key="reset_filters", use_container_width=True):
        for k in ('filter_symbol', 'filter_direction', 'filter_date_from', 'filter_date_to'):
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

st.markdown(f"<hr style='border-color: {BORDER}; margin: 12px 0;'>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────
# Filter Helper
# ──────────────────────────────────────────────────────────
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Filtre bar'a göre DataFrame'i filtrele."""
    if df.empty:
        return df
    if filter_symbol:
        df = df[df['symbol'].str.upper().str.contains(filter_symbol, na=False)]
    if filter_direction != "Tümü":
        df = df[df['trade_type'] == filter_direction]
    if filter_date_from:
        df = df[pd.to_datetime(df['entry_date']).dt.date >= filter_date_from]
    if filter_date_to:
        df = df[pd.to_datetime(df['entry_date']).dt.date <= filter_date_to]
    return df.reset_index(drop=True)

# ──────────────────────────────────────────────────────────
# AÇIK POZİSYONLAR
# ──────────────────────────────────────────────────────────
open_df = get_trades(status="Open")
if not open_df.empty:
    open_df = open_df[open_df['status'] != 'Deleted']  # soft-deleted'ları filtrele
open_df = apply_filters(open_df)

st.markdown(f"### 🟢 Açık Pozisyonlar ({len(open_df)})")

if open_df.empty:
    st.info("📭 Açık pozisyon yok. Yeni pozisyon eklemek için **Yeni Pozisyon** sayfasına gidin.")
else:
    # Long ve Short olarak ayır
    long_df = open_df[open_df['trade_type'] == 'Long'].reset_index(drop=True)
    short_df = open_df[open_df['trade_type'] == 'Short'].reset_index(drop=True)

    def render_open_section(df: pd.DataFrame, chip_html: str):
        """Bir Long veya Short bölümünü render et."""
        if df.empty:
            return
        st.markdown(f"<div style='margin: 14px 0 8px;'>{chip_html} <span style='color:{TEXT_MUTED}; margin-left:8px;'>({len(df)} pozisyon)</span></div>", unsafe_allow_html=True)

        # Header row
        h = st.columns([1.2, 0.7, 0.9, 1.0, 1.0, 0.9, 1.0, 1.0, 1.0, 0.5, 0.5, 0.5])
        headers = ["Sembol", "Yön", "Adet", "Giriş", "Stop", "DTS%", "Risk $", "Strateji", "Tarih", "✏️", "🚪", "🗑️"]
        for col, hdr in zip(h, headers):
            col.markdown(f"<div style='color:{TEXT_MUTED}; font-size:11px; font-weight:600;'>{hdr}</div>", unsafe_allow_html=True)

        # Rows
        for _, row in df.iterrows():
            tid = int(row['id'])
            entry_p = float(row['entry_price'] or 0)
            stop_p = float(row['stop_loss'] or 0)
            qty = float(row['quantity'] or 0)
            risk = float(row['risk_amount'] or 0)

            # DTS%: Long = (entry-stop)/entry, Short = (stop-entry)/entry
            if entry_p > 0 and stop_p > 0:
                if row['trade_type'] == 'Long':
                    dts_pct = (entry_p - stop_p) / entry_p * 100
                else:
                    dts_pct = (stop_p - entry_p) / entry_p * 100
            else:
                dts_pct = 0

            entry_date_str = ''
            if row.get('entry_date'):
                try:
                    entry_date_str = pd.to_datetime(row['entry_date']).strftime('%Y-%m-%d %H:%M')
                except Exception:
                    entry_date_str = str(row['entry_date'])

            c = st.columns([1.2, 0.7, 0.9, 1.0, 1.0, 0.9, 1.0, 1.0, 1.0, 0.5, 0.5, 0.5])
            c[0].markdown(f"**{row['symbol']}**")
            c[1].markdown(chip_long('L') if row['trade_type'] == 'Long' else chip_short('S'), unsafe_allow_html=True)
            c[2].markdown(f"{qty:,.0f}")
            c[3].markdown(f"${entry_p:,.2f}")
            c[4].markdown(f"${stop_p:,.2f}")
            c[5].markdown(colored_pct(dts_pct, decimals=2), unsafe_allow_html=True)
            c[6].markdown(f"<span style='color:{DANGER}'>${risk:,.2f}</span>", unsafe_allow_html=True)
            c[7].markdown(f"<span style='color:{TEXT_MUTED}; font-size:12px;'>{row.get('strategy', '') or '-'}</span>", unsafe_allow_html=True)
            c[8].markdown(f"<span style='color:{TEXT_MUTED}; font-size:12px;'>{entry_date_str}</span>", unsafe_allow_html=True)

            # Aksiyon butonları
            if c[9].button("✏️", key=f"edit_btn_{tid}", help="Düzenle"):
                _set_action('edit', tid)
                st.rerun()
            if c[10].button("🚪", key=f"close_btn_{tid}", help="Kapat"):
                _set_action('close', tid)
                st.rerun()
            if c[11].button("🗑️", key=f"delete_btn_{tid}", help="Sil"):
                _set_action('delete', tid)
                st.rerun()

    render_open_section(long_df, chip_long("🟢 LONG"))
    render_open_section(short_df, chip_short("🔴 SHORT"))

# ──────────────────────────────────────────────────────────
# KAPALI POZİSYONLAR
# ──────────────────────────────────────────────────────────
st.markdown(f"<hr style='border-color: {BORDER}; margin: 24px 0 12px;'>", unsafe_allow_html=True)
closed_df = get_trades(status="Closed")
if not closed_df.empty:
    closed_df = closed_df[closed_df['status'] != 'Deleted']
closed_df = apply_filters(closed_df)

st.markdown(f"### 🔴 Geçmiş Pozisyonlar ({len(closed_df)})")

if closed_df.empty:
    st.markdown(f"<p style='color:{TEXT_MUTED}; font-size:13px;'>Henüz kapanan pozisyon yok.</p>", unsafe_allow_html=True)
else:
    h = st.columns([1.2, 0.7, 0.9, 1.0, 1.0, 1.1, 1.0, 0.7, 0.7, 1.0])
    headers = ["Sembol", "Yön", "Adet", "Giriş", "Çıkış", "P&L $", "P&L %", "R", "Süre", "Tarih"]
    for col, hdr in zip(h, headers):
        col.markdown(f"<div style='color:{TEXT_MUTED}; font-size:11px; font-weight:600;'>{hdr}</div>", unsafe_allow_html=True)

    for _, row in closed_df.iterrows():
        entry_p = float(row['entry_price'] or 0)
        exit_p = float(row['exit_price'] or 0)
        qty = float(row['quantity'] or 0)
        pnl_d = float(row['profit_loss'] or 0)
        pnl_p = float(row['pnl_pct'] or 0)
        r_mult = float(row['r_multiple'] or 0)

        duration = '-'
        try:
            ed = pd.to_datetime(row['entry_date'])
            xd = pd.to_datetime(row['exit_date'])
            if pd.notna(ed) and pd.notna(xd):
                duration = f"{(xd - ed).days}g"
        except Exception:
            pass

        exit_date_str = ''
        if row.get('exit_date'):
            try:
                exit_date_str = pd.to_datetime(row['exit_date']).strftime('%Y-%m-%d %H:%M')
            except Exception:
                exit_date_str = str(row['exit_date'])

        pnl_color = SUCCESS if pnl_d >= 0 else DANGER

        c = st.columns([1.2, 0.7, 0.9, 1.0, 1.0, 1.1, 1.0, 0.7, 0.7, 1.0])
        c[0].markdown(f"**{row['symbol']}**")
        c[1].markdown(chip_long('L') if row['trade_type'] == 'Long' else chip_short('S'), unsafe_allow_html=True)
        c[2].markdown(f"{qty:,.0f}")
        c[3].markdown(f"${entry_p:,.2f}")
        c[4].markdown(f"${exit_p:,.2f}")
        c[5].markdown(f"<span style='color:{pnl_color}; font-weight:600;'>${pnl_d:,.2f}</span>", unsafe_allow_html=True)
        c[6].markdown(colored_pct(pnl_p, decimals=2), unsafe_allow_html=True)
        c[7].markdown(f"<span style='color:{pnl_color};'>{r_mult:+.2f}R</span>", unsafe_allow_html=True)
        c[8].markdown(f"<span style='color:{TEXT_MUTED}; font-size:12px;'>{duration}</span>", unsafe_allow_html=True)
        c[9].markdown(f"<span style='color:{TEXT_MUTED}; font-size:12px;'>{exit_date_str}</span>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────
# MODALS (st.session_state ile)
# ──────────────────────────────────────────────────────────
st.markdown(f"<hr style='border-color: {BORDER}; margin: 24px 0 12px;'>", unsafe_allow_html=True)

def _get_trade_by_id(trade_id: int) -> pd.Series:
    """ID'ye göre tek trade döndürür."""
    df = get_trades(status=None)  # tüm trade'ler
    if df.empty:
        return None
    matches = df[df['id'] == trade_id]
    if matches.empty:
        return None
    return matches.iloc[0]

# ── EDIT MODAL ──
if st.session_state['edit_trade_id'] is not None:
    tid = st.session_state['edit_trade_id']
    trade = _get_trade_by_id(tid)
    if trade is None:
        st.error("Pozisyon bulunamadı.")
        _clear_action_state()
        st.rerun()
    else:
        st.markdown(f"### ✏️ Pozisyon Düzenle — {trade['symbol']}")
        with st.form(key=f"edit_form_{tid}", clear_on_submit=False):
            ec1, ec2 = st.columns(2)
            with ec1:
                e_qty = st.number_input("Adet", min_value=1.0, value=float(trade['quantity']), step=1.0)
                e_entry = st.number_input("Giriş Fiyatı ($)", min_value=0.01, value=float(trade['entry_price']), step=0.01, format="%.4f")
                e_stop = st.number_input("Stop Loss ($)", min_value=0.01, value=float(trade['stop_loss']), step=0.01, format="%.4f")
            with ec2:
                e_strategy = st.text_input("Strateji", value=trade.get('strategy') or '')
                e_commission = st.number_input("Komisyon ($)", min_value=0.0, value=float(trade.get('commission') or 0), step=0.01, format="%.2f")
                e_notes = st.text_area("Notlar", value=trade.get('notes') or '', height=100)

            col_a, col_b = st.columns(2)
            submit_edit = col_a.form_submit_button("💾 Değişiklikleri Kaydet", type="primary", use_container_width=True)
            cancel_edit = col_b.form_submit_button("❌ İptal", use_container_width=True)

            if submit_edit:
                update_data = {
                    'quantity': e_qty,
                    'entry_price': e_entry,
                    'stop_loss': e_stop,
                    'strategy': e_strategy,
                    'commission': e_commission,
                    'notes': e_notes,
                }
                if update_trade(tid, update_data):
                    st.success(f"✅ {trade['symbol']} pozisyonu güncellendi.")
                    _clear_action_state()
                    st.rerun()
                else:
                    st.error("❌ Güncelleme başarısız oldu. Logları kontrol edin.")
            elif cancel_edit:
                _clear_action_state()
                st.rerun()

# ── CLOSE MODAL ──
if st.session_state['close_trade_id'] is not None:
    tid = st.session_state['close_trade_id']
    trade = _get_trade_by_id(tid)
    if trade is None:
        st.error("Pozisyon bulunamadı.")
        _clear_action_state()
        st.rerun()
    else:
        st.markdown(f"### 🚪 Pozisyon Kapat — {trade['symbol']}")
        with st.form(key=f"close_form_{tid}", clear_on_submit=False):
            cc1, cc2 = st.columns(2)
            with cc1:
                close_exit_price = st.number_input(
                    "Çıkış Fiyatı ($) *",
                    min_value=0.01,
                    value=float(trade['entry_price']),
                    step=0.01,
                    format="%.4f"
                )
                ce1, ce2 = st.columns([2, 1])
                with ce1:
                    close_exit_date = st.date_input("Çıkış Tarihi", value=date.today())
                with ce2:
                    close_exit_time = st.time_input(
                        "Çıkış Saati",
                        value=datetime.now().time().replace(second=0, microsecond=0),
                    )
                close_exit_datetime = datetime.combine(close_exit_date, close_exit_time)
            with cc2:
                # Live P&L preview
                entry_p = float(trade['entry_price'])
                qty = float(trade['quantity'])
                comm = float(trade.get('commission') or 0)
                if trade['trade_type'] == 'Short':
                    preview_pnl = (entry_p - close_exit_price) * qty - comm
                else:
                    preview_pnl = (close_exit_price - entry_p) * qty - comm
                preview_pct = (preview_pnl / (entry_p * qty)) * 100 if entry_p * qty > 0 else 0
                pnl_color = SUCCESS if preview_pnl >= 0 else DANGER
                st.markdown(f"""
                <div style='background:{SURFACE}; border:1px solid {BORDER}; border-radius:8px; padding:14px; margin-top:8px;'>
                    <div style='color:{TEXT_MUTED}; font-size:12px;'>Tahmini P&L</div>
                    <div style='color:{pnl_color}; font-size:22px; font-weight:700;'>${preview_pnl:,.2f}</div>
                    <div style='color:{pnl_color}; font-size:13px;'>{preview_pct:+.2f}%</div>
                </div>
                """, unsafe_allow_html=True)

            close_notes = st.text_area("Çıkış Notu (opsiyonel)", placeholder="Çıkış sebebi, gözlem, ders...", height=80)

            col_a, col_b = st.columns(2)
            submit_close = col_a.form_submit_button("🚪 Pozisyonu Kapat", type="primary", use_container_width=True)
            cancel_close = col_b.form_submit_button("❌ İptal", use_container_width=True)

            if submit_close:
                if close_trade(tid, close_exit_price, close_exit_datetime, close_notes if close_notes else None):
                    st.success(f"✅ {trade['symbol']} pozisyonu kapatıldı. P&L: ${preview_pnl:,.2f}")
                    st.balloons()
                    _clear_action_state()
                    st.rerun()
                else:
                    st.error("❌ Kapatma başarısız oldu. Logları kontrol edin.")
            elif cancel_close:
                _clear_action_state()
                st.rerun()

# ── DELETE CONFIRM ──
if st.session_state['delete_confirm_id'] is not None:
    tid = st.session_state['delete_confirm_id']
    trade = _get_trade_by_id(tid)
    if trade is None:
        st.error("Pozisyon bulunamadı.")
        _clear_action_state()
        st.rerun()
    else:
        st.markdown(f"""
        <div style='background:{SURFACE}; border:2px solid {WARNING}; border-radius:12px; padding:20px; margin:16px 0;'>
            <div style='color:{WARNING}; font-size:18px; font-weight:700; margin-bottom:8px;'>⚠️ Pozisyonu Silmek İstediğine Emin Misin?</div>
            <div style='color:{TEXT}; font-size:14px;'>
                <strong>{trade['symbol']}</strong> — {trade['trade_type']} — {float(trade['quantity']):,.0f} adet @ ${float(trade['entry_price']):,.2f}
            </div>
            <div style='color:{TEXT_MUTED}; font-size:12px; margin-top:8px;'>
                Bu işlem soft-delete'tir (status='Deleted'). Veri kaybedilmez ama pozisyon listelerden kaldırılır.
            </div>
        </div>
        """, unsafe_allow_html=True)

        dc1, dc2 = st.columns(2)
        if dc1.button("✅ Evet, Sil", key=f"confirm_delete_{tid}", type="primary", use_container_width=True):
            if delete_trade(tid, soft=True):
                st.success(f"✅ {trade['symbol']} pozisyonu silindi.")
                _clear_action_state()
                st.rerun()
            else:
                st.error("❌ Silme başarısız oldu.")
        if dc2.button("❌ İptal", key=f"cancel_delete_{tid}", use_container_width=True):
            _clear_action_state()
            st.rerun()
