"""
Yeni Pozisyon — Trade entry formu + Live Risk Calculator.

Minervini PA1 referansından + Markets 360 risk calculator formüllerinden
hibrit. Portfolio değeri portfolios tablosundan okunur, üstte gösterilir,
edit modal'ı ile güncellenebilir.
"""

import streamlit as st
from datetime import date, datetime, time
from styles import (
    apply_styles, card_title,
    SUCCESS, DANGER, TEXT, TEXT_MUTED, SURFACE, BORDER
)
from db_connection import (
    insert_trade, get_portfolio, update_portfolio, update_portfolio_starting
)
from quanfina_math import suggest_entry_grade

st.set_page_config(page_title="Yeni Pozisyon — Quanfina", layout="wide")
apply_styles()

# ════════════════════════════════════════════════════════════════
# 1. PORTFÖY ŞERİDİ (üstte)
# ════════════════════════════════════════════════════════════════
portfolio = get_portfolio(1)
if portfolio is None:
    st.error("Portföy bulunamadı. Cloud SQL bağlantısını kontrol edin.")
    st.stop()

starting_value = portfolio["starting_value"]
current_value  = portfolio["current_value"]
ytd_pct = ((current_value - starting_value) / starting_value * 100) if starting_value else 0

# Şerit kart
st.markdown(f"""
<div style="
    background: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: 12px;
    padding: 12px 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
">
    <div style="display: flex; gap: 24px; align-items: center;">
        <span style="color: {TEXT_MUTED}; font-size: 13px;">💼 Portföy</span>
        <span style="color: {TEXT}; font-size: 22px; font-weight: 700;">
            ${current_value:,.2f}
        </span>
        <span style="color: {SUCCESS if ytd_pct >= 0 else DANGER}; font-size: 14px;">
            YTD {'+'if ytd_pct >= 0 else ''}{ytd_pct:.2f}%
        </span>
    </div>
    <div style="color: {TEXT_MUTED}; font-size: 12px;">
        Başlangıç: ${starting_value:,.2f}
    </div>
</div>
""", unsafe_allow_html=True)

# Edit butonu (expander içinde)
with st.expander("✏️ Portföy Değerini Düzenle", expanded=False):
    col_a, col_b = st.columns(2)
    with col_a:
        new_starting = st.number_input(
            "Başlangıç Değeri (YTD baseline)",
            value=float(starting_value),
            min_value=0.0,
            step=100.0,
            format="%.2f",
            key="edit_starting",
        )
    with col_b:
        new_current = st.number_input(
            "Anlık Değer",
            value=float(current_value),
            min_value=0.0,
            step=100.0,
            format="%.2f",
            key="edit_current",
        )
    if st.button("💾 Portföyü Güncelle", type="primary"):
        if new_starting != starting_value:
            update_portfolio_starting(1, new_starting)
        if new_current != current_value:
            update_portfolio(1, new_current)
        st.success("Portföy güncellendi.")
        st.rerun()

st.markdown(f"### {card_title('Yeni Pozisyon')}", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# 2. FORM (sol) + LIVE CALCULATOR (sağ)
# ════════════════════════════════════════════════════════════════
col_form, col_calc = st.columns([1.2, 1])

with col_form:
    # Form alanları
    col1, col2 = st.columns(2)
    with col1:
        et1, et2 = st.columns([2, 1])
        with et1:
            entry_date = st.date_input("Giriş Tarihi", value=date.today(), key="entry_date")
        with et2:
            entry_time = st.time_input(
                "Giriş Saati",
                value=datetime.now().time().replace(second=0, microsecond=0),
                key="entry_time",
            )
        entry_datetime = datetime.combine(entry_date, entry_time)
        trade_type = st.radio(
            "Yön",
            options=["Long", "Short"],
            horizontal=True,
            index=0,
        )
        symbol = st.text_input("Sembol *", placeholder="AAPL").upper().strip()
        strategy = st.selectbox(
            "Strateji",
            options=["Minervini VCP", "Minervini Pivot", "Carr Setup #1", "Diğer"],
        )
    with col2:
        quantity = st.number_input("Adet *", min_value=0, step=1, value=0)
        entry_price = st.number_input(
            "Giriş Fiyatı *", min_value=0.0, step=0.01, format="%.2f", value=0.0
        )
        stop_loss = st.number_input(
            "Stop Loss *", min_value=0.0, step=0.01, format="%.2f", value=0.0
        )
        commission = st.number_input(
            "Komisyon", min_value=0.0, step=0.01, format="%.2f", value=0.0
        )
        pivot_price = st.number_input(
            "Pivot Fiyatı ($)",
            min_value=0.0,
            value=0.0,
            step=0.01,
            format="%.2f",
            help="TradeGrader önerisi için (opsiyonel) — VCP/Setup pivot fiyatı. 0 bırakırsanız öneri gösterilmez.",
        )

    notes = st.text_area("Not", placeholder="Pozisyon notu (isteğe bağlı)", height=80)

with col_calc:
    st.markdown(
        f'<div style="color: {TEXT_MUTED}; font-size: 13px; margin-bottom: 12px;">'
        f'📊 Risk Calculator (canlı)</div>',
        unsafe_allow_html=True,
    )

    # ─── Hesaplama ───
    valid_inputs = (entry_price > 0 and stop_loss > 0 and quantity > 0)
    if valid_inputs:
        # Long/Short'a göre risk yönü
        if trade_type == "Long":
            stop_amount_per_share = entry_price - stop_loss
            valid_direction = stop_loss < entry_price
        else:  # Short
            stop_amount_per_share = stop_loss - entry_price
            valid_direction = stop_loss > entry_price

        if valid_direction and stop_amount_per_share > 0:
            position_size_dollars = quantity * entry_price
            risk_dollars   = quantity * stop_amount_per_share
            risk_pct       = (stop_amount_per_share / entry_price) * 100
            risk_equity_pct = (risk_dollars / current_value) * 100 if current_value else 0
            position_size_pct = (position_size_dollars / current_value) * 100 if current_value else 0
            breakeven      = entry_price + (commission * 2 / quantity) if quantity else entry_price
            sbe_pct        = ((breakeven - entry_price) / entry_price) * 100

            # Calculator kart
            st.markdown(f"""
            <div style="background:{SURFACE}; border:1px solid {BORDER};
                        border-radius:12px; padding:16px;">
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; font-size:13px;">
                    <div>
                        <div style="color:{TEXT_MUTED};">Pozisyon Tutarı</div>
                        <div style="color:{TEXT}; font-size:18px; font-weight:600;">
                            ${position_size_dollars:,.2f}
                        </div>
                    </div>
                    <div>
                        <div style="color:{TEXT_MUTED};">$ Risk</div>
                        <div style="color:{DANGER}; font-size:18px; font-weight:600;">
                            ${risk_dollars:,.2f}
                        </div>
                    </div>
                    <div>
                        <div style="color:{TEXT_MUTED};">Risk %</div>
                        <div style="color:{TEXT};">{risk_pct:.2f}%</div>
                    </div>
                    <div>
                        <div style="color:{TEXT_MUTED};">Risk Equity %</div>
                        <div style="color:{TEXT};">{risk_equity_pct:.2f}%</div>
                    </div>
                    <div>
                        <div style="color:{TEXT_MUTED};">Pozisyon %</div>
                        <div style="color:{TEXT};">{position_size_pct:.2f}%</div>
                    </div>
                    <div>
                        <div style="color:{TEXT_MUTED};">SBE %</div>
                        <div style="color:{TEXT};">{sbe_pct:.4f}%</div>
                    </div>
                    <div style="grid-column:1/-1; padding-top:8px;
                                border-top:1px solid {BORDER};">
                        <div style="color:{TEXT_MUTED}; font-size:12px;">
                            Başabaş Fiyatı
                        </div>
                        <div style="color:{TEXT}; font-size:15px;">
                            ${breakeven:.4f}
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.warning(
                f"⚠️ {'Long' if trade_type == 'Long' else 'Short'} için "
                f"{'stop < giriş' if trade_type == 'Long' else 'stop > giriş'} olmalı."
            )
    else:
        st.info(
            f"💡 Sembol, adet, giriş fiyatı ve stop loss girin — "
            f"otomatik hesaplama burada gösterilir."
        )

# ════════════════════════════════════════════════════════════════
# 3. SUBMIT
# ════════════════════════════════════════════════════════════════
st.markdown("---")
col_submit, col_clear, _ = st.columns([1, 1, 2])

with col_submit:
    submit = st.button("💾 Pozisyonu Kaydet", type="primary", use_container_width=True)

with col_clear:
    if st.button("🔄 Temizle", use_container_width=True):
        st.rerun()

if submit:
    # Validation
    errors = []
    if not symbol:
        errors.append("Sembol gerekli")
    if quantity <= 0:
        errors.append("Adet > 0 olmalı")
    if entry_price <= 0:
        errors.append("Giriş fiyatı > 0 olmalı")
    if stop_loss <= 0:
        errors.append("Stop loss > 0 olmalı")
    if trade_type == "Long" and stop_loss >= entry_price:
        errors.append("Long için stop < giriş olmalı")
    if trade_type == "Short" and stop_loss <= entry_price:
        errors.append("Short için stop > giriş olmalı")

    if errors:
        for err in errors:
            st.error(f"❌ {err}")
    else:
        # Hesapları tekrar yap (kayıt için)
        if trade_type == "Long":
            stop_amount_per_share = entry_price - stop_loss
        else:
            stop_amount_per_share = stop_loss - entry_price

        position_size_dollars = quantity * entry_price
        risk_dollars  = quantity * stop_amount_per_share
        risk_pct      = (stop_amount_per_share / entry_price) * 100
        risk_equity_pct = (risk_dollars / current_value) * 100 if current_value else 0
        position_size_pct = (position_size_dollars / current_value) * 100 if current_value else 0
        breakeven    = entry_price + (commission * 2 / quantity) if quantity else entry_price
        sbe_pct      = ((breakeven - entry_price) / entry_price) * 100

        data = {
            "symbol":                symbol,
            "trade_type":            trade_type,
            "invest_type":           1 if trade_type == "Long" else 2,
            "strategy":              strategy,
            "entry_date":            entry_datetime,
            "entry_price":           entry_price,
            "stop_loss":             stop_loss,
            "quantity":              quantity,
            "risk_amount":           risk_dollars,
            "risk_pct":              risk_pct,
            "risk_equity_pct":       risk_equity_pct,
            "position_size_pct":     position_size_pct,
            "position_size_dollars": position_size_dollars,
            "breakeven":             breakeven,
            "sbe_pct":               sbe_pct,
            "commission":            commission,
            "portfolio_id":          1,
            "notes":                 notes if notes else None,
            "status":                "Open",
        }

        try:
            insert_trade(data)
            direction_icon = "🟢" if trade_type == "Long" else "🔴"
            st.markdown(f"""
    <div style="
        background: rgba(88, 189, 125, 0.12);
        border-left: 3px solid {SUCCESS};
        padding: 12px 16px;
        border-radius: 8px;
        margin-top: 12px;
        color: {TEXT};
    ">
        ✅ <strong>Pozisyon kaydedildi:</strong> {symbol} {direction_icon} {trade_type}
        · {quantity} adet · <span style="color:{DANGER};">${risk_dollars:.2f}</span> risk
    </div>
    """, unsafe_allow_html=True)
            st.balloons()
            if pivot_price > 0:
                suggestion = suggest_entry_grade(entry_price, pivot_price)
                if suggestion.code:
                    confidence_emoji = {"HIGH": "🎯", "MEDIUM": "📊", "LOW": "💭"}.get(
                        suggestion.confidence, "📊"
                    )
                    message = (
                        f"{confidence_emoji} **TradeGrader Entry Önerisi:** "
                        f"`{suggestion.code}` — {suggestion.name} "
                        f"({suggestion.confidence} güven)\n\n"
                        f"💡 {suggestion.reason}"
                    )
                    if suggestion.code == "BP":
                        st.success(message)
                    elif suggestion.code in ("BL", "CE"):
                        st.warning(message)
                    else:
                        st.info(message)
        except Exception as e:
            st.error(f"❌ Kayıt hatası: {e}")
