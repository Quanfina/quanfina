"""
2_Screens.py — Hisse Keşfetme Sayfası (28 Screen Kataloğu)

Sn. Ferit'in ana ihtiyacı: "Hangi hisseyi alacağım?"
3_Minervini.py'dan bağımsız, genel keşif aracı.

Kullanıcı bir kategori + screen seçer → eşleşen hisseleri görür →
"Add to Watch" ile 3_Minervini watch listesine yollar.

Sprint progress:
- 4-bis.1a [BU]: UI iskeleti + dummy data
- 4-bis.1b: 7 ready screen — gerçek SQL
- 4-bis.1c: "Add to Watch" entegrasyonu
- 4-bis.2: parse-bazlı (Loose, Buy Risk, 5-Kriter Momentum, Qualifier, Tight) — 9 screen
- 4-bis.3: scan_diff (Moving Up, Jump 2+, D to B+, New RPR×3) — 6 screen
- 4-bis.4: near_ready (IPO×3 + TPR D Below 200d) — 4 screen
- 4-bis.5+: complex (Stage 2 ETFs) — 1 screen
"""

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Screens", page_icon="🔎", layout="wide")
st.title("🔎 Hisse Keşfetme — 28 Screen Kataloğu")

# ═════════════════════════════════════════════════════════════
# SCREENS — C1 Notebook L428-469'dan birebir kopya
# (kod, ad, kategori, durum, filtre_açıklaması, hisse_sayısı_referans)
# ═════════════════════════════════════════════════════════════

SCREENS = [
    # Kategori A — TPR-Bazlı (8)
    ("bread_butter",       "Bread & Butter",                "TPR-Bazlı", "out_of_mvp",  "Mark'ın günlük rutin (kriter belirsiz)",        307),
    ("tpr_a",              "TPR A",                         "TPR-Bazlı", "ready",       "grade = 'A'",                                   216),
    ("tpr_a_b",            "TPR A & B",                     "TPR-Bazlı", "ready",       "grade IN ('A','B')",                            370),
    ("tpr_moving_up",      "TPR Moving Up",                 "TPR-Bazlı", "scan_diff",   "Önceki scan'le karşılaştırma (grade)",          101),
    ("tpr_jump_2",         "TPR Jump 2 or More Grades",     "TPR-Bazlı", "scan_diff",   "Önceki scan'le karşılaştırma (>=2 sıçrama)",    27),
    ("tpr_d_to_b",         "TPR D to B+",                   "TPR-Bazlı", "scan_diff",   "Önceki scan'le karşılaştırma (D→B/A)",          5),
    ("tpr_d_below_200",    "TPR D - Below 200d",            "TPR-Bazlı", "near_ready",  "grade='D' + price<ma200 (ma200 yazılmalı)",     953),
    ("rpr_89_tpr_c",       "RPR 89+ TPR C+",                "TPR-Bazlı", "ready",       "rs_ibd >= 89 AND grade IN ('A','B','C')",       398),

    # Kategori B — Stage-Bazlı (7)
    ("stage2_10p",         "Stage 2 ($10+)",                "Stage",     "ready",       "passed=1 AND price >= 10",                      404),
    ("stage2_etfs_5p",     "Stage 2 ETFs ($5+)",            "Stage",     "complex",     "ETF flag eksik (ayrı scan akışı)",              77),
    ("stage2_below_10",    "Stage 2 (Below $10)",           "Stage",     "ready",       "passed=1 AND price < 10",                       114),
    ("stage2_loose_10p",   "Stage 2 Loose ($10+)",          "Stage",     "parse",       "confirmations parse (≥6/8)",                    92),
    ("stage2_loose_below", "Stage 2 Loose (Below $10)",     "Stage",     "parse",       "confirmations parse (≥6/8)",                    29),
    ("stage2_vloose_10p",  "Stage 2 Very Loose ($10+)",     "Stage",     "parse",       "confirmations parse (≥4/8)",                    152),
    ("stage2_vloose_below","Stage 2 Very Loose (Below $10)","Stage",     "parse",       "confirmations parse (≥4/8)",                    33),

    # Kategori C — RPR-Bazlı (4)
    ("top5_rpr",           "Top 5% RPR",                    "RPR-Bazlı", "ready",       "rs_ibd >= 95",                                  107),
    ("new_top5_rpr",       "New Top 5% RPR",                "RPR-Bazlı", "scan_diff",   "Önceki scan'de < 95, şimdi >= 95",              14),
    ("new_7d_rpr_10p",     "New 7D RPR ($10+)",             "RPR-Bazlı", "scan_diff",   "7 gün önceki scan'le karşılaştırma",            12),
    ("new_7d_rpr_below",   "New 7D RPR (Below $10)",        "RPR-Bazlı", "scan_diff",   "Aynı (< $10)",                                  3),

    # Kategori D — IPO-Bazlı (3)
    ("ipos_stocks",        "IPOs (Stocks)",                 "IPO",       "near_ready",  "Finviz c=70 ekle (~1 satır)",                   105),
    ("ipos_etfs",          "IPOs (ETFs)",                   "IPO",       "near_ready",  "c=70 + ETF flag",                               133),
    ("power_ipo",          "Power IPO",                     "IPO",       "near_ready",  "c=70 + RS eşiği",                               4),

    # Kategori E — Pattern (3)
    ("tight_low_volume",   "Tight Price Low Volume",        "Pattern",   "parse",       "price_volume_history JSONB üzerinden hesap",    85),
    ("buy_risk_green",     "Buy Risk Green",                "Pattern",   "parse",       "(conf - viol) sayısı + eşik (belirsiz)",        585),
    ("momentum_5x_rpr_70", "5-Kriter Momentum (RPR 70+)",   "Pattern",   "parse",       "rs_ibd>=70 + 5-kriter momentum pattern (kısmen açık)", 339),

    # Kategori F — Momentum (3)
    ("mom_10p",            "Minervini Momentum ($10+)",     "Momentum",  "ready",       "passed=1 AND price >= 10 (≈ Stage 2 strict)",   286),
    ("mom_below_10",       "Minervini Momentum (Below $10)","Momentum",  "ready",       "passed=1 AND price < 10 (≈ Stage 2 strict)",    139),
    ("mom_qualifier",      "Minervini Qualifier",           "Momentum",  "parse",       "Tam kriter belirsiz (passed=1 yaklaşımı)",      501),
]

# Durum etiketi → (emoji, kısa ad, sprint hedef bilgisi)
STATUS_BADGE = {
    "ready":       ("✅", "Hazır",         "MVP'ye dahil (4-bis.1b)"),
    "near_ready":  ("🟡", "Yakın",          "Sprint 4-bis.4 (küçük scanner refactor)"),
    "parse":       ("⚙️", "Parse Gerekli",  "Sprint 4-bis.2"),
    "scan_diff":   ("📊", "Scan Diff",      "Sprint 4-bis.3"),
    "complex":     ("🔴", "Karmaşık",       "Sprint 4-bis.5+"),
    "out_of_mvp":  ("⏸️", "MVP Dışı",       "Kriter belirsiz (NotebookLM Konu 23)"),
}

CATEGORIES = ["TPR-Bazlı", "Stage", "RPR-Bazlı", "IPO", "Pattern", "Momentum"]

# ═════════════════════════════════════════════════════════════
# UI — Üst özet
# ═════════════════════════════════════════════════════════════

ready_count    = sum(1 for s in SCREENS if s[3] == "ready")
near_ready_count = sum(1 for s in SCREENS if s[3] == "near_ready")
total          = len(SCREENS)

col_m1, col_m2, col_m3, col_m4 = st.columns(4)
col_m1.metric("Toplam Screen", total)
col_m2.metric("✅ Hazır", ready_count)
col_m3.metric("🟡 Yakın", near_ready_count)
col_m4.metric("⏳ Beklemede", total - ready_count - near_ready_count)

st.caption(
    f"Sprint 4-bis.1a — UI iskeleti (dummy data). "
    f"{ready_count}/{total} screen Sprint 4-bis.1b'de gerçek SQL ile çalışacak. "
    f"{near_ready_count} screen 4-bis.4'te küçük scanner refactor sonrası aktif olur."
)

st.divider()

# ═════════════════════════════════════════════════════════════
# UI — Kategori + Screen seçici
# ═════════════════════════════════════════════════════════════

col_cat, col_screen = st.columns([1, 2])

with col_cat:
    selected_category = st.radio(
        "Kategori",
        options=CATEGORIES,
        key="screens_category",
    )

filtered_screens = [s for s in SCREENS if s[2] == selected_category]

with col_screen:
    if not filtered_screens:
        st.info("Bu kategoride screen yok.")
        selected_screen = None
    else:
        screen_labels = [
            f"{STATUS_BADGE[s[3]][0]} {s[1]} ({s[5]})"
            for s in filtered_screens
        ]
        screen_idx = st.radio(
            "Screen",
            options=range(len(filtered_screens)),
            format_func=lambda i: screen_labels[i],
            key="screens_selected",
        )
        selected_screen = filtered_screens[screen_idx]

st.divider()

# ═════════════════════════════════════════════════════════════
# UI — Seçilen screen detayı + dummy tablo
# ═════════════════════════════════════════════════════════════

if selected_screen:
    code, name, category, status, filter_desc, ref_count = selected_screen
    badge_emoji, badge_text, badge_note = STATUS_BADGE[status]

    st.subheader(f"{badge_emoji} {name}")

    info_col1, info_col2, info_col3 = st.columns(3)
    info_col1.markdown(f"**Kategori:** {category}")
    info_col2.markdown(f"**Durum:** {badge_text}")
    info_col3.markdown(f"**Tarama ref:** {ref_count} hisse")

    st.markdown(f"**Filtre:** `{filter_desc}`")
    st.caption(f"📌 {badge_note}")

    if status == "ready":
        st.info(
            "🚧 Sprint 4-bis.1b'de gerçek SQL ile çalışacak. "
            "Şu an dummy veri gösteriliyor."
        )

        # Dummy data — Sprint 4-bis.1b'de SCREEN_QUERIES dict'inden gelecek
        dummy_df = pd.DataFrame({
            "Ticker":    ["DUMMY1", "DUMMY2", "DUMMY3"],
            "Şirket":    ["Test Co A", "Test Co B", "Test Co C"],
            "Fiyat":     [42.50, 18.75, 156.20],
            "% Değişim": [1.2, -0.5, 2.8],
            "Sektör":    ["Technology", "Healthcare", "Industrial"],
            "TPR":       ["A", "B", "A"],
            "RPR":       [92, 87, 96],
        })
        st.dataframe(dummy_df, use_container_width=True, hide_index=True)

        st.button(
            "➕ Watch'a Ekle",
            disabled=True,
            help="Sprint 4-bis.1c'de aktif olacak",
        )

    elif status == "out_of_mvp":
        st.warning(
            f"⏸️ **MVP dışı:** {filter_desc}. "
            f"Tam kriter Bundle'da bulunamadı (server-side). NotebookLM Konu 23 kayıtlı."
        )

    elif status == "near_ready":
        st.warning(
            f"🟡 **Yakın hazır:** {filter_desc}. "
            f"Sprint 4-bis.4'te scanner.py'a küçük bir refactor (1-10 satır) "
            f"yapıldıktan sonra aktif olacak."
        )

    elif status == "scan_diff":
        st.warning(
            f"📊 **Scan-to-scan karşılaştırma:** {filter_desc}. "
            f"Sprint 4-bis.3'te SQL window function ile çözülecek (mevcut veriyle)."
        )

    elif status == "parse":
        st.warning(
            f"⚙️ **Parse gerekli:** {filter_desc}. "
            f"Sprint 4-bis.2'de TEXT/JSONB parsing ile çözülecek."
        )

    elif status == "complex":
        st.warning(
            f"🔴 **Karmaşık:** {filter_desc}. "
            f"Yeni veri akışı gerektiriyor (ETF tespit). Sprint 4-bis.5+."
        )

# ═════════════════════════════════════════════════════════════
# Alt bilgi — Tüm screen durum tablosu
# ═════════════════════════════════════════════════════════════

with st.expander("📊 28 Screen — Tam Durum Tablosu"):
    summary_df = pd.DataFrame(
        [
            {
                "Kod":      s[0],
                "Ad":       s[1],
                "Kategori": s[2],
                "Durum":    f"{STATUS_BADGE[s[3]][0]} {STATUS_BADGE[s[3]][1]}",
                "Sprint":   STATUS_BADGE[s[3]][2],
                "Ref Hisse": s[5],
            }
            for s in SCREENS
        ]
    )
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
