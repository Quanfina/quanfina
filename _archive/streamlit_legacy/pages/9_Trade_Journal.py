import streamlit as st
import pandas as pd
from datetime import date
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection import (
    insert_journal,
    get_journal,
    get_trades,
    get_grade_categories,
    get_all_leg_exits_for_trade,
    update_leg_exit_grade,
    get_legs_for_trade,
    update_leg_grade,
)
from quanfina_math import GRADE_CATEGORIES, suggest_entry_grade
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
    st.subheader("🎯 Trade Notları")
    st.caption(
        "Geçmiş kapalı pozisyonlarına TradeGrader kategorisi ata. "
        "Yeni trade'ler 7_Pozisyonlar'da kapatıldığında otomatik öneri alır; "
        "burası geçmiş trade'leri toplu inceleme/atama için."
    )

    closed_df = get_trades(status="Closed")

    if closed_df.empty:
        st.info("📭 Henüz kapalı trade yok. Trade kapattığında burada görünecek.")
    else:
        # Özet bar
        _total = len(closed_df)
        _wins = int((closed_df["pnl_pct"] > 0).sum()) if "pnl_pct" in closed_df.columns else 0
        _losses = _total - _wins
        _win_rate = (_wins / _total * 100) if _total > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Toplam", _total)
        c2.metric("Kazanan", _wins)
        c3.metric("Kaybeden", _losses)
        c4.metric("Win Rate", f"{_win_rate:.1f}%")

        st.divider()

        # Filtre bar
        fc1, fc2 = st.columns([1, 1])
        with fc1:
            grade_filter = st.selectbox(
                "Grade Durumu",
                options=["Tümü", "Gradelenmemiş", "Gradelenmiş"],
                index=0,
                key="tj_grade_filter",
            )
        with fc2:
            symbol_filter = st.text_input(
                "Symbol (opsiyonel)",
                value="",
                placeholder="örn: AAPL",
                key="tj_symbol_filter",
            ).strip().upper()

        # Trade listesi + grade durumu hesabı
        _display_rows = []
        _grade_status = {}

        for _, t in closed_df.iterrows():
            tid = int(t["id"])
            symbol = str(t.get("symbol", "?"))

            if symbol_filter and symbol_filter not in symbol.upper():
                continue

            legs = get_all_leg_exits_for_trade(tid)
            exit_count = len(legs)
            graded_count = sum(1 for leg in legs if leg.get("grade_category_id") is not None)
            all_graded = exit_count > 0 and graded_count == exit_count

            _grade_status[tid] = {
                "all_graded": all_graded,
                "exit_count": exit_count,
                "graded_count": graded_count,
            }

            if grade_filter == "Gradelenmemiş":
                if exit_count == 0 or all_graded:
                    continue
            elif grade_filter == "Gradelenmiş":
                if not all_graded:
                    continue

            if exit_count == 0:
                status_icon = "—"
            elif all_graded:
                status_icon = "✅"
            else:
                status_icon = f"⏳ {graded_count}/{exit_count}"

            pnl = t.get("pnl_pct")
            _display_rows.append({
                "ID": tid,
                "Sembol": symbol,
                "Tip": t.get("trade_type", "?"),
                "Giriş": t.get("entry_date"),
                "Çıkış": t.get("exit_date"),
                "PnL %": f"{float(pnl):.2f}" if pd.notna(pnl) else "—",
                "Grade": status_icon,
            })

        if not _display_rows:
            st.info("Filtreye uyan trade bulunamadı.")
        else:
            st.dataframe(
                pd.DataFrame(_display_rows),
                use_container_width=True,
                hide_index=True,
            )

            st.divider()
            st.markdown("##### 📂 Trade Detayı / Grade Atama")

            _trade_labels = [f"#{r['ID']} — {r['Sembol']} ({r['Çıkış']})" for r in _display_rows]
            _selected_idx = st.selectbox(
                "Detay görüntülenecek trade",
                options=range(len(_trade_labels)),
                format_func=lambda i: _trade_labels[i],
                key="tj_selected_trade",
            )

            if _selected_idx is not None:
                selected_tid = _display_rows[_selected_idx]["ID"]
                selected_trade = closed_df[closed_df["id"] == selected_tid].iloc[0]

                with st.expander(
                    f"📊 Trade #{selected_tid} — {selected_trade.get('symbol')} detayı",
                    expanded=True,
                ):
                    dc1, dc2, dc3, dc4 = st.columns(4)
                    entry_p = selected_trade.get("entry_price")
                    stop_p = selected_trade.get("stop_loss")
                    exit_p = selected_trade.get("exit_price")
                    pnl_p = selected_trade.get("pnl_pct")
                    dc1.metric("Giriş", f"${float(entry_p):.2f}" if pd.notna(entry_p) else "—")
                    dc2.metric("Stop", f"${float(stop_p):.2f}" if pd.notna(stop_p) else "—")
                    dc3.metric("Çıkış", f"${float(exit_p):.2f}" if pd.notna(exit_p) else "—")
                    dc4.metric("PnL %", f"{float(pnl_p):.2f}%" if pd.notna(pnl_p) else "—")

                    notes_val = selected_trade.get("notes")
                    if notes_val and pd.notna(notes_val):
                        st.caption(f"📝 Not: {notes_val}")

                    st.divider()

                    legs = get_all_leg_exits_for_trade(selected_tid)

                    if not legs:
                        st.warning(
                            "⚠️ Bu trade'in leg_exits kaydı yok. "
                            "Eski pattern ile kapatılmış (Sprint 4.7d.1 öncesi). "
                            "Grade atanamaz."
                        )
                    else:
                        st.markdown("**Leg Exits ve Grade Atamaları**")

                        all_cats = get_grade_categories()
                        eligible_cats = [c for c in all_cats if c["leg_type"] in ("EXIT", "LOSS")]
                        cat_codes = [c["code"] for c in eligible_cats]
                        code_to_id = {c["code"]: c["id"] for c in eligible_cats}
                        code_to_name = {c["code"]: c["name"] for c in eligible_cats}

                        with st.form(key=f"grade_form_{selected_tid}"):
                            new_grades = {}
                            new_annotations = {}

                            for leg in legs:
                                lid = leg["id"]
                                exit_idx = leg.get("exit_idx", "?")
                                shares = leg.get("shares") or 0
                                price = leg.get("price") or 0
                                current_code = leg.get("grade_code")
                                current_annotation = leg.get("annotation") or ""

                                st.markdown(
                                    f"**Exit #{exit_idx}** — {float(shares):.0f} adet @ ${float(price):.2f}"
                                    + (f" — Mevcut: `{current_code}`" if current_code else " — *Grade atanmamış*")
                                )

                                options = ["(Atla)"] + cat_codes
                                default_idx = 0
                                if current_code and current_code in cat_codes:
                                    default_idx = cat_codes.index(current_code) + 1

                                selected = st.selectbox(
                                    "Grade",
                                    options=options,
                                    index=default_idx,
                                    format_func=lambda x: x if x == "(Atla)" else f"{x} — {code_to_name.get(x, x)}",
                                    key=f"tj_grade_sel_{lid}",
                                    label_visibility="collapsed",
                                )

                                annotation = st.text_input(
                                    "Açıklama (opsiyonel)",
                                    value=current_annotation,
                                    key=f"tj_annot_{lid}",
                                    placeholder="Örn: 'duygusal sat — RSI uyarısı vardı'",
                                )

                                new_grades[lid] = None if selected == "(Atla)" else code_to_id.get(selected)
                                new_annotations[lid] = annotation if annotation.strip() else None

                                st.divider()

                            saved = st.form_submit_button("💾 Grade'leri Kaydet", use_container_width=True)

                        if saved:
                            update_count = 0
                            for lid, cat_id in new_grades.items():
                                if cat_id is not None:
                                    ok = update_leg_exit_grade(lid, cat_id, new_annotations.get(lid))
                                    if ok:
                                        update_count += 1
                            if update_count > 0:
                                st.success(f"✅ {update_count} grade kaydedildi.")
                                st.rerun()
                            else:
                                st.warning("Hiç grade seçilmedi veya kayıt başarısız.")

                    st.divider()
                    st.markdown("**Giriş Legi Gradeleri**")

                    _entry_legs = get_legs_for_trade(selected_tid)

                    if not _entry_legs:
                        st.caption("Bu trade'in kayıtlı giriş legi yok.")
                    else:
                        _all_cats_full = get_grade_categories()
                        entry_cats = [c for c in _all_cats_full if c["leg_type"] == "ENTRY"]
                        entry_codes = [c["code"] for c in entry_cats]
                        entry_code_to_id = {c["code"]: c["id"] for c in entry_cats}
                        entry_code_to_name = {c["code"]: c["name"] for c in entry_cats}

                        with st.form(key=f"entry_leg_grade_form_{selected_tid}"):
                            new_leg_grades = {}
                            new_leg_annotations = {}

                            for leg in _entry_legs:
                                lid = leg["id"]
                                current_code = leg.get("grade_code")

                                col_info, col_pivot = st.columns([2, 1])
                                with col_info:
                                    st.markdown(
                                        f"**Leg #{leg['leg_idx']}** — "
                                        f"{float(leg['shares']):.0f} adet @ ${float(leg['price']):.2f}"
                                        + (f" — Mevcut: `{current_code}`" if current_code else " — *Grade atanmamış*")
                                    )
                                with col_pivot:
                                    pivot_in = st.number_input(
                                        "Pivot $",
                                        min_value=0.0,
                                        value=0.0,
                                        step=0.01,
                                        key=f"tj_entry_pivot_{lid}",
                                        help="Pivot fiyatı gir → otomatik öneri çıkar. 0 = öneri yok.",
                                    )

                                sug = None
                                if pivot_in > 0:
                                    try:
                                        _s = suggest_entry_grade(float(leg["price"]), pivot_in)
                                        if _s.confidence != "LOW":
                                            sug = _s
                                    except Exception:
                                        sug = None

                                options = ["(Atla)"] + entry_codes
                                default_idx = 0
                                if sug and sug.code in entry_codes:
                                    default_idx = entry_codes.index(sug.code) + 1
                                elif current_code and current_code in entry_codes:
                                    default_idx = entry_codes.index(current_code) + 1

                                selected = st.selectbox(
                                    "Grade",
                                    options=options,
                                    index=default_idx,
                                    format_func=lambda x: x if x == "(Atla)" else f"{x} — {entry_code_to_name.get(x, x)}",
                                    key=f"tj_entry_grade_{lid}",
                                    label_visibility="collapsed",
                                )
                                if sug:
                                    st.caption(f"💡 Öneri: `{sug.code}` ({sug.confidence}) — {sug.reason}")

                                annotation = st.text_input(
                                    "Açıklama (opsiyonel)",
                                    value=leg.get("annotation") or "",
                                    key=f"tj_entry_annot_{lid}",
                                    placeholder="örn: pivot kırılımında temiz giriş",
                                )

                                new_leg_grades[lid] = None if selected == "(Atla)" else entry_code_to_id.get(selected)
                                new_leg_annotations[lid] = annotation.strip() or None
                                st.divider()

                            entry_saved = st.form_submit_button(
                                "💾 Giriş Grade'lerini Kaydet",
                                use_container_width=True,
                            )

                        if entry_saved:
                            update_count = 0
                            for lid, cat_id in new_leg_grades.items():
                                if cat_id is not None:
                                    ok = update_leg_grade(lid, cat_id, new_leg_annotations.get(lid))
                                    if ok:
                                        update_count += 1
                            if update_count > 0:
                                st.success(f"✅ {update_count} giriş legi grade kaydedildi.")
                                st.rerun()
                            else:
                                st.warning("Hiç grade seçilmedi veya kayıt başarısız.")

with tab_plan:
    st.subheader("Trading Plan ve Kurallar")
    st.info("Sistemin değişmez kurallarını buraya listeleyeceğiz. Her işleme girmeden önce buradan teyit edeceksin.")

with tab_hata:
    st.subheader("Hata Kataloğu")
    st.info("Tekrar eden hatalarını (örn: FOMO, Erken Çıkış, Stop Kaydırma) buradan etiketleyip istatistiklerini tutacağız.")

with tab_aylik:
    st.subheader("Aylık Öz-Değerlendirme")
    st.info("Her ayın sonunda R-Multiple performansını ve psikolojik gelişimini burada değerlendireceksin.")