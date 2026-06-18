/**
 * Sprint 4-bis.6 Screen tipler — ARŞİV MODU
 *
 * 22 May 2026: 25 screen kaldirildi (UI'dan). Tam envanter:
 *   notebook/Tarama_Listeleri_Arsiv.md
 *
 * Sn. Ferit talimat (22 May 2026): "Adim adim gidicem artik. Notebook kaydet,
 * tarama sayfasindaki listeleri de kaydederek kaldir."
 *
 * Sebep: 5-Kaynak danisma sentezi (Sprint_4_bis_5_Kaynak_Sentez.md). Karar
 * yorgunlugu somut kanit (Sn. Ferit Vizyon: "Sayfaya bakiyorum neye baktigimi
 * anlamiyorum"). Kural #18 pasif oge cikarma somut canli.
 *
 * Backend KORUNDU:
 *   - scanner.py 1955 satir
 *   - quanfina_math.py 903 satir (9 compute_* fonksiyon)
 *   - api/db_helpers.py SCREENS_READY_12 + SCREENS_PARSE_7 + SCREENS_DIFF_6
 *   - api/main.py /api/screens/{slug} endpoint
 *   - DB tablolari (minervini_scans + 6 Migration kolonlari)
 *
 * Frontend (bu dosya): ScreenSlug union BOSALDI, SCREEN_CATEGORIES = {}.
 * Hook'lar (useScreenMeta, useScreenResults) cagrilmaz hale geldi - screens/page.tsx
 * basit bos state gosterir.
 *
 * Yeniden doldurma plani: Sprint 4-bis.7+ olay-tetikli sirali
 *   1. Cup with Handle (yeni screen)
 *   2. Pocket Pivot (yeni screen)
 *   3. Flat Base (Stage 2 alt-filtre)
 *
 * Geri getirme: notebook/Tarama_Listeleri_Arsiv.md "Geri Getirme Talimati"
 *
 * KARAR ref: #402, #461, #465, #466, #467, KARAR ADAY #442, #468
 * Kural ref: #4, #18, #23
 * Ilke ref: #4, #11
 */

// ScreenSlug union ARSIVDE - tum 25 slug notebook/Tarama_Listeleri_Arsiv.md'de
// Tip korunur (hook'lar icin), ileride yeni setup eklendikce union dolar.
export type ScreenSlug = string;

// Sprint 4-bis.4 KARAR #461: "deferred" kategorisi bosaldi.
// Sprint 4-bis.6 (22 May 2026): tum kategoriler de bosaldi (Kural #18 pasif oge cikarma).
export type ScreenCategory = "ready" | "parse" | "diff" | "deferred";

export interface ScreenMeta {
  slug: ScreenSlug;
  label: string;
  filter_summary: string;
  category?: ScreenCategory;
}

export interface ScreenResultRow {
  symbol: string;
  grade: string | null;
  rs_ibd: number | null;
  price: number | null;
  passed: number | null;
  scan_date: string | null;
  // KARAR #466 — VCP Kalite Skoru (slug yeniden eklenince anlamli)
  vcp_quality_score?: "EXCELLENT" | "PASS" | null;
  // KARAR #465 — VCP Ready Score 0-100
  vcp_ready_score?: number | null;
  // KARAR #467 — Power Play (HTF) Mark resmi kuralı
  power_play_pass?: boolean | null;
  // KARAR ADAY #893 — Mark Tennis Ball Detector (TLSMW s.253) — backend Migration 007 sonrasi
  tennis_ball_pattern?: "TENNIS_BALL" | "partial" | "none" | null;
  // KARAR ADAY #882 — Mark Volume Asymmetry (TLSMW s.234) — backend Migration 007 sonrasi
  volume_asymmetry_tier?: "healthy" | "neutral" | "distribution" | null;
  // KARAR ADAY #713 — Mark Code 33 (TLSMW s.173 EPS+Sales+Margin triple) — yfinance pipeline (AÇIK KONU #75) sonrasi
  code_33_pattern?: "CODE_33" | "partial" | "none" | null;
  // KARAR ADAY #735 — Carr Stage rozet (Mark+Carr birleşik)
  carr_stage?: 1 | 2 | 3 | 4 | null;
  // KARAR #733 alt-paket (Paket 83, 26 May 2026): Pivot Breakout status
  // P81 Sinyaller + P82 Watchlist paten — Tarama'da AL/Zayıf/Yakın/Altı kolon
  pivot_status?: "CONFIRMED" | "WEAK" | "NEAR_PIVOT" | "BELOW_PIVOT" | null;
  // P423 (31 May 2026 — Kural #28): grade='D' SADECE eps/sales verisi yok demekse
  // true (gerçekten zayıf DEĞİL). UI gri "D" + "veri yok" tooltip; gerçek zayıf
  // D kırmızı kalır. Backend db_helpers._grade_no_data hesabı.
  grade_no_data?: boolean | null;
}

/**
 * Kategoriler — Sprint 4-bis.7 Adim Adim Yeniden Ekleme (22 May 2026)
 * Tum 25 eski slug -> kategori esleme notebook/Tarama_Listeleri_Arsiv.md'de.
 *
 * Yeniden ekleme: bu objeye slug + kategori ekle, dropdown'da gozukur.
 * Backend zaten /api/screens + /api/screens/{slug} canli (db_helpers.SCREENS_READY_9).
 *
 * Adim 1 (22 May 2026): stage2_10p — Minervini Trend Template (Mark resmi kuralı 8 kosul).
 * Notebook_A satir 846 kitap birebir: passed=1 = Trend Template PASS.
 * Web Claude 5-Kaynak sentez: "Must-Have Setup #1 ZATEN CANLI."
 */
export const SCREEN_CATEGORIES: Record<string, string> = {
  stage2_10p: "Trend Template",
  temel_eleme: "Temel Eleme",
  tam_minervini: "Tam Minervini Tarama",
  // 18 Haz 2026 — Geçerli Baz re-add. Olay tetikleyici (arşiv planı madde 1/3):
  // P490 sequence/NaN fix sonrası base kolonları (cup/flat/double quality) canlı doldu.
  // Vizyon "geçerli baz filtresi" (birleşik) — Cup/Flat/Double EXCELLENT+GOOD.
  valid_base: "Geçerli Baz",
  // 18 Haz 2026 — Minervini-core önemli taramalar arşivden geri (canlı sayım doğrulandı):
  top5_rpr: "Top %5 RS (Liderler)",          // RS≥95, 38 hisse — Mark #1 lider kriteri
  power_play_ready: "Power Play (HTF)",       // KARAR #467, 25 hisse — Mark canon momentum
  tight_low_vol_excellent: "VCP EXCELLENT",   // KARAR #466, 3 hisse — Mark imza paterni A+
  // 18 Haz 2026 — 2. parti önemli (canlı sayım doğrulandı):
  new_top5_rpr: "Yeni Top %5 RS",            // DIFF: prev RS<95 -> RS≥95, 4 hisse (yeni lider)
  healthy_accumulation: "Sağlıklı Toplama",   // volume_asymmetry healthy, 33 hisse (kurumsal)
  rpr_89_tpr_c: "RS 89+ Grade A/B/C",        // RS≥89 + grade, 38 hisse (lider + temel kalite)
  // 18 Haz 2026 — yüksek-conviction İZLEME ekranları (bugün 0; nadir güçlü setup çıkınca
  // görünür — alarm değeri). Kolonlar DOLU (vcp_ready max 66<70 canon; bugün TENNIS_BALL yok).
  vcp_ready_high: "VCP Ready (70+)",         // KARAR #465 — Inside Day + V-Dry + Tight ≥70
  tennis_ball_active: "🎾 Tennis Ball",       // KARAR ADAY #893 — pullback+recovery (TLSMW s.253)
  // 18 Haz 2026 — Carr Mean Reversion bulk screen (P502). pvh-hesap, scanner/deploy yok.
  mean_reversion: "Mean Reversion (Carr)",   // Carr 2.baskı s.356 countertrend LONG — çekirdek setup
  // 18 Haz 2026 — Carr Coiled Spring bulk screen (P511). pvh 80≥60 → bulk mümkün.
  coiled_spring: "Coiled Spring (Carr)",     // Carr 2.baskı s.250 daralan yay TIER-2 ADAY (eyeball şart)
  // 18 Haz 2026 — Carr 200-261 bar bulk screen (P516). pvh 80→280 sonrası (gelecek scan'de dolar).
  pullback: "Pullback (Carr)",               // Carr s.249 WEAK_bull trend-takip
  blue_sky: "Blue Sky Breakout (Carr)",      // Carr s.264 STRONG+WEAK breakout
  bullish_base: "Bullish Base (Carr)",       // Carr s.291 CONTRARIAN downtrend baz TIER-2
  bullish_divergence: "Bullish Divergence (Carr)",  // Carr s.258 uptrend-dip TIER-2
  // 18 Haz 2026 — Carr SHORT katalog (P518). Blue Sky aynası, strong-bear.
  blue_sea: "Blue Sea Breakdown (Carr SHORT)",  // Carr s.289 strong-bear breakdown SHORT
  // 18 Haz 2026 — Carr SHORT katalog (P520). Ralli-sonu reversal.
  gap_down: "Gap Down (Carr SHORT)",            // Carr s.273 ralli-sonu reversal SHORT
  // 18 Haz 2026 — Carr SHORT katalog SON (P522). Yükselen kama kırılımı.
  rising_wedge: "Rising Wedge (Carr SHORT)",    // Carr Böl.19 kama kırılımı SHORT (TIER-2)
};

// SCREEN_DESCRIPTIONS kaldirildi (22 May 2026) — Sn. Ferit "bunu sil" talimati.
// Kosullar listesi yeterli aciklayici, ek metin gerekmiyor. Gelecekte istenirse
// tekrar eklenir.

/**
 * Filtre kosullari — Mark resmi kuralı eşikler liste halinde.
 * Kullanici tam olarak hangi kurallarin uygulandigini gorur.
 * Kaynak: notebook/kitaplar/Minervini.md (Trade Like a Stock Market Wizard).
 *
 * stage2_10p — Trade Like a Stock Market Wizard, Bölüm 5 (s. 79) birebir sırası.
 * Kitap madde sırası = Mark Minervini'nin önem hiyerarşisi.
 *
 * Her koşul kaynak tipiyle etiketlenir:
 *   - "mark"     : Mark Minervini orijinal kuralı (kitap birebir)
 *   - "quanfina" : Quanfina ek filtre (Mark'ın kuralı değil, sistem eklemesi)
 */
// 22 May 2026 — 3 kategori revize (Sn. Ferit talimatı):
// - quanfina: Evren daraltma (Fiyat $10 + Hacim 500K) — UI 1-2. sıra
// - mark: Mark Resmi Kural (kitap birebir 8 madde) — UI 3-10. sıra
// - mark_ekstra: Mark kitap tavsiyesi (preferably/ideally şartları) — UI 11. sıra, TURUNCU
export type ConditionSource = "mark" | "quanfina" | "mark_ekstra" | "carr";

export interface ScreenCondition {
  source: ConditionSource;
  text: string;
}

export const SCREEN_CONDITIONS: Record<string, ScreenCondition[]> = {
  stage2_10p: [
    // 22 May 2026 — Sn. Ferit yeniden sıralama:
    // 1-2: Quanfina Ek (evren daraltma — fiyat + hacim ÖNCE)
    // 3-10: Mark Resmi Kural (kitap birebir 8 madde, Trade Like a Wizard s.79)
    // 11: Mark Ekstra Kural (kitap tavsiyesi — TURUNCU, "preferably/ideally" şartları)
    //
    // Kanon kaynak: Quanfina Minervini + Quanfina Notebook çift doğrulama
    // (22 May 2026 NotebookLM kitap birebir kanıt zinciri).
    { source: "quanfina", text: "Fiyat ≥ $10 (mikro-cap eleme — evren daraltma)" },
    { source: "quanfina", text: "Ortalama hacim ≥ 500.000 (likidite eleme — evren daraltma)" },
    { source: "mark", text: "Fiyat > 150 ve 200 günlük hareketli ortalama (150/200 DMA)" },
    { source: "mark", text: "150DMA > 200DMA (uzun vadeli yükseliş yapısı)" },
    { source: "mark", text: "200DMA en az 1 aydır yukarı yönlü (trending up)" },
    { source: "mark", text: "50DMA > 150DMA > 200DMA (sıralı piramit dizilimi)" },
    { source: "mark", text: "Fiyat > 50 günlük hareketli ortalama (50DMA)" },
    { source: "mark", text: "Fiyat 52 haftalık dipten en az %30 yukarıda" },
    { source: "mark", text: "Fiyat 52 haftalık zirveye en fazla %25 mesafede" },
    { source: "mark", text: "Relative Strength (RS) sıralaması ≥ 70 (IBD)" }, // Mark'ın 8. resmi maddesi
    // 11. Mark Ekstra Kural — Mark kitap tavsiyesi (kitap birebir ZORUNLU değil ama tercih)
    // Kitap birebir: "RS Rating ≥ 70 (ideali 80-90+)" — Mark daha güçlü adayları
    // 80-90+ RS'de arar. Trend Template'in zorunlu eşiği 70 ama tercih 80-90+.
    { source: "mark_ekstra", text: "RS Rating ideali 80-90+ (Mark kitap tavsiyesi — daha güçlü adaylar)" },
  ],

  // 23 May 2026 — Temel Eleme şablonu (Mark Minervini Fundamental — Soft Score)
  // Sn. Ferit talimat (23 May): "bu 5 koşulu yapalım trend template gibi"
  // 24 May 2026 revize — Gem + Quanfina Notebook Çift Danışma:
  //   Mark felsefesi gereği Fundamental kuralları "Hard Filter" DEĞİL,
  //   "Soft Score / Relative Prioritizing" katmanına aittir.
  //   Yeni IPO + biotech + Story Stocks "Açıklanamayan Güç" hisseleri
  //   Hard Filter'a takılırsa Quanfina patlayıcı fırsatları eler.
  //
  // Kaynak zinciri (KALICI İLKE #4):
  //   1-2: Quanfina Ek (evren daraltma — Mark sapma)
  //   3:   EPS Growth Q/Q ≥ %25 — TLSMW s.127
  //   4:   Sales Growth Q/Q ≥ %25 — TLSMW s.132 (ideal üç haneli)
  //   5:   ROE ≥ %15-17 — Momentum Masters s.74
  //
  // Hepsi Mark Ekstra (turuncu) — Soft Score katmanı (Mark "Recipe")
  // Backend scanner.py get_finviz_fundamental_only Quanfina pratik gereği
  // Hard Cut uyguluyor (sapma kaydı), UI'da Mark felsefesine sadık turuncu.
  temel_eleme: [
    { source: "quanfina", text: "Fiyat ≥ $10 (mikro-cap eleme — evren daraltma)" },
    { source: "quanfina", text: "Ortalama hacim ≥ 500.000 (likidite eleme — evren daraltma)" },
    { source: "mark_ekstra", text: "EPS Q/Q artışı ≥ %25 (TLSMW s.127 — Mark Soft Score, Recipe)" },
    { source: "mark_ekstra", text: "Sales Q/Q artışı ≥ %25 (TLSMW s.132 — Soft Score, ideal üç haneli)" },
    { source: "mark_ekstra", text: "ROE ≥ %15-17 (Momentum Masters s.74 — Soft Score, yeni IPO için esnetilir)" },
  ],

  // 18 Haz 2026 — Geçerli Baz (Cup-with-Handle / Flat Base / Double Bottom birleşik)
  // O'Neil baz analizi (Minervini Stage 2 setup'ın temeli). Backend kolonları:
  // cup_handle_quality / flat_base_quality / double_bottom_quality — quanfina_math
  // compute_cup_with_handle / compute_flat_base / compute_double_bottom (O'Neil referanslı).
  // "Geçerli" = en az birinde EXCELLENT veya GOOD; MARGINAL (kusurlu, "O'Neil dikkatli")
  // ve NONE hariç (Minervini hard-cut felsefesi — bkz. SCREENS_DIFF_6 "saf geçiş yeterli").
  valid_base: [
    { source: "mark", text: "Cup-with-Handle, Flat Base veya Double Bottom baz formasyonu tespit edildi (O'Neil baz analizi)" },
    { source: "mark", text: "Baz kalitesi EXCELLENT veya GOOD (kusurlu MARGINAL + formasyon yok NONE hariç — hard-cut)" },
  ],

  // 18 Haz 2026 — Minervini-core önemli taramalar (arşivden geri, canlı sayım doğrulandı)
  top5_rpr: [
    { source: "mark", text: "Relative Strength (RS) ≥ 95 — en güçlü %5 lider (Mark: 'en güçlüyü al', TLSMW lider önceliği)" },
  ],
  power_play_ready: [
    { source: "mark", text: "Power Play / High Tight Flag: POLE %100+ yükseliş + FLAG %10-25 sıkı bayrak (Mark canon, KARAR #467)" },
  ],
  tight_low_vol_excellent: [
    { source: "mark", text: "VCP Kalite = EXCELLENT (A+): en sıkı daralma (<%50) + hacim kuruması + bonus teyit (Mark VCP canon, KARAR #466)" },
  ],
  new_top5_rpr: [
    { source: "mark", text: "Yeni lider: önceki taramada RS < 95 iken şimdi RS ≥ 95 + fiyat ≥ $10 (Mark erken liderlik yakalama)" },
  ],
  healthy_accumulation: [
    { source: "mark", text: "Up-volume >> Down-volume — kurumsal toplama (Mark Volume Asymmetry, TLSMW s.234)" },
  ],
  rpr_89_tpr_c: [
    { source: "mark", text: "Relative Strength (RS) ≥ 89 + Grade A/B/C (güçlü lider + temel kalite teyidi)" },
  ],
  vcp_ready_high: [
    { source: "mark", text: "VCP Ready Score ≥ 70 — Inside Day + Volume Dry-up + Tight daralma sentezi (Mark VCP hazırlık, KARAR #465)" },
  ],
  tennis_ball_active: [
    { source: "mark", text: "Tennis Ball: breakout sonrası sağlıklı pullback + hızlı recovery — 'top değil tenis topu' (Mark TLSMW s.253, KARAR ADAY #893)" },
  ],
  // 18 Haz 2026 — Carr Mean Reversion (s.356) countertrend LONG; pvh-hesap bulk screen
  mean_reversion: [
    { source: "carr", text: "Countertrend LONG (Carr 2.baskı s.356): dün alt-BB altı → bugün alt-BB üstü dönüş + bugün close < SMA20×0.9 + dün bearish mum + bugün < dün open" },
    { source: "carr", text: "Stop yapısal ×0.985 + %8 hard cap (s.410); hedef SMA20 dinamik (s.339); 7-gün time stop (s.400). Gösterge SADECE BB(20,2.0)+SMA20+candle" },
  ],
  // 18 Haz 2026 — Carr Coiled Spring (s.250) TIER-2 daralan yay ADAYI; pvh-hesap bulk screen
  coiled_spring: [
    { source: "carr", text: "Daralan yay LONG ADAYI (Carr 2.baskı s.250): high≤3/7g önce + low≥3/7g önce (range daralıyor) + 20g yüksek≥60g + close>SMA20 + SMA20>SMA50×1.03 + bugün yeşil" },
    { source: "carr", text: "TIER-2 EYEBALL ŞART (s.248-249): trendline kırılımı + yukarı eğim DEĞİL + hiçbir kısım 50MA'ya değmez. GÖSTERGE YOK (pure pattern). Stop 50MA-%2/%8 cap, hedef 2R" },
  ],
  // 18 Haz 2026 — Carr 200-261 bar setup'lar bulk (P516, pvh 80→280 sonrası dolar)
  pullback: [
    { source: "carr", text: "WEAK_bull trend-takip LONG (Carr 2.baskı s.249): SMA20>SMA50>SMA200 + 50/20 yükseliyor + Stoch %K(5,3,3)<20 (oversold pullback) + bugün close>open" },
    { source: "carr", text: "ENTRY ertesi gün signal high üstü buy-stop (s.248). Stop 50MA-%2 / %8 cap (s.321), hedef 2R (s.324). Time stop YOK (trend trailing)" },
  ],
  blue_sky: [
    { source: "carr", text: "STRONG+WEAK breakout LONG (Carr 2.baskı s.264): 40g yeni yüksek + 52h zirve ALTI + close≤260g min low×2 + OBV/MACD 40g yeni yüksek + bugün yeşil. SMA YOK" },
    { source: "carr", text: "ENTRY ertesi gün signal high üstü (s.335). Stop %6 / %8 cap (s.325, Ch22 evrensel), hedef 2R (s.324). Time stop YOK" },
  ],
  bullish_base: [
    { source: "carr", text: "CONTRARIAN downtrend-baz LONG ADAYI (Carr 2.baskı s.291): SMA50>SMA20 (downtrend) + 50 düşüyor + range daralma(30>15>5) + OBV/MACD yükseliyor + SMA50<SMA200 + bugün yeşil" },
    { source: "carr", text: "TIER-2 EYEBALL (s.287-288): base tipi falling wedge/expanding triangle/rectangle — RISING WEDGE OLMAZ. Kırılım BEKLENMEZ → entry=close (s.284). Stop %6/%8 cap, hedef 2R" },
  ],
  bullish_divergence: [
    { source: "carr", text: "Uptrend-dip LONG ADAYI (Carr 2.baskı s.258): SMA50>SMA200 + fiyat lower low + 6 göstergeden 2+'si higher low (MACD line/hist, Stoch%K, RSI(5), CCI(20), OBV) + bugün yeşil" },
    { source: "carr", text: "TIER-2 EYEBALL (s.256): divergence gözle teyit (fiyat lower low + gösterge higher low). entry=close (s.262), stop sell-off son dibi/%8 cap, hedef 2R. Uzun tutma sabrı (s.252)" },
  ],
  // 18 Haz 2026 — Carr Blue Sea Breakdown SHORT (s.289); Blue Sky aynası, pvh-hesap bulk
  blue_sea: [
    { source: "carr", text: "strong-bear SHORT (Carr 2.baskı s.289): 40g yeni DÜŞÜK + 52h dip DEĞİL + close>0.8×52h ZİRVE (asimetri s.286 — %20'den fazla düşmemiş) + OBV/MACD 40g yeni düşük + bugün kırmızı" },
    { source: "carr", text: "ENTRY ertesi gün signal low altı sell-stop (s.317). Stop %6 ÜSTTE / %8 cap (s.306), hedef 2R AŞAĞI (s.324). SADECE strong-bear (s.283). Quanfina long-biased — bilgi amaçlı" },
  ],
  // 18 Haz 2026 — Carr Gap Down SHORT (s.273); ralli-sonu reversal, pvh-hesap bulk
  gap_down: [
    { source: "carr", text: "ralli-sonu reversal SHORT (Carr 2.baskı s.273): high[bugün]<low[dün]×0.99 (unfilled gap ≥%1, s.270) + kırmızı mum + 50MA 20/40/60g yükseliyor + fiyat dün/20g/40g önce 50MA üstü. GÖSTERGE YOK (s.275 pure price)" },
    { source: "carr", text: "ENTRY gap günü close (market emri, s.270 — signal low BEKLEMEZ). HABER TEYİDİ şart (s.272: gap şirkete özel kötü haberden mi?). Stop %6 ÜSTTE / 2R AŞAĞI (s.305,324). Bullish/Range piyasa (s.266)" },
  ],
  // 18 Haz 2026 — Carr Rising Wedge Breakdown SHORT (Böl.19); pvh-hesap bulk (son Carr setup)
  rising_wedge: [
    { source: "carr", text: "yükselen kama kırılımı SHORT (Carr 2.baskı Böl.19): SMA50 40g yükseliyor (uptrend) + range daralma (30>15>5, kama) + MACD 40g DÜŞÜYOR + OBV 40g DÜŞÜYOR (bearish divergence) + kırmızı mum" },
    { source: "carr", text: "TIER-2 EYEBALL: kama trendline (≥3 dokunuş) + OBV trendline aşağı kırılımı → ilk kırmızı mum (entry). MACD lower-highs / fiyat higher-highs divergence. Stop %6 ÜSTTE / 2R AŞAĞI. Bullish/Range" },
  ],

  // 24 May 2026 — Tam Minervini Tarama (Hibrit Pipeline: 10 Hard + 5 Soft)
  // Sn. Ferit talimat: "tam minervini tarama listesi yapıcaz hem teknik
  //                     hem temel kaynaklara danışarak"
  // Çift Danışma sonucu (Gem 01_Minervini_Uzmanı + Quanfina Notebook):
  //   AŞAMA 1 — SCREEN (Hard Filter, 10 madde): 2 Quanfina + 8 Mark Resmi Teknik
  //   AŞAMA 2 — RECIPE (Soft Score, 5 madde):   EPS Q/Q + Sales Q/Q + ROE +
  //                                             Yıllık EPS + Operating Margin
  //
  // Mark Pipeline (TLSMW Bölüm 5 + 7 + Momentum Masters):
  //   10.000 Universe → Trend Template (Hard) → ~1.000 → Fundamentals (Soft) → 40-100
  //
  // Backend: minervini_fundamental_scans tablosu (scanner.py get_finviz_fundamental)
  // Yıllık EPS + Operating Margin GELİŞTİRİLMESİ LAZIM — Migration sonra,
  // şu an UI'da gösterilir, backend ileride filter olarak uygulanır.
  // RS Multi-Month gereksiz (sistem 6 RS varyantı zaten hesaplıyor).
  tam_minervini: [
    // AŞAMA 1 — SCREEN (Hard Filter) — 10 madde
    { source: "quanfina", text: "Fiyat ≥ $10 (mikro-cap eleme — evren daraltma)" },
    { source: "quanfina", text: "Ortalama hacim ≥ 500.000 (likidite eleme — evren daraltma)" },
    { source: "mark", text: "Fiyat > 150 ve 200 günlük hareketli ortalama (TLSMW s.79)" },
    { source: "mark", text: "150DMA > 200DMA (uzun vadeli yükseliş yapısı)" },
    { source: "mark", text: "200DMA en az 1 aydır yukarı yönlü (tercihen 4-5 ay)" },
    { source: "mark", text: "50DMA > 150DMA > 200DMA (sıralı piramit dizilimi)" },
    { source: "mark", text: "Fiyat > 50 günlük hareketli ortalama (50DMA)" },
    { source: "mark", text: "Fiyat 52 haftalık dipten en az %30 yukarıda (TLSMW s.79 birebir)" },
    { source: "mark", text: "Fiyat 52 haftalık zirveye en fazla %25 mesafede" },
    { source: "mark", text: "Relative Strength (RS) sıralaması ≥ 70 (IBD)" },
    // AŞAMA 2 — RECIPE (Soft Score) — 5 madde (Mark Ekstra turuncu)
    { source: "mark_ekstra", text: "EPS Q/Q artışı ≥ %25 (TLSMW s.127 — Soft Score)" },
    { source: "mark_ekstra", text: "Sales Q/Q artışı ≥ %25 veya 3 çeyrek ivmelenme (TLSMW s.132)" },
    { source: "mark_ekstra", text: "ROE ≥ %15-17 (Momentum Masters s.74 — yeni IPO esnetilir)" },
    { source: "mark_ekstra", text: "Yıllık EPS pozitif / Breakout Year (TLSMW s.134-136)" },
    { source: "mark_ekstra", text: "Operating Margin expanding (TLSMW s.145-147 — sabit eşik yok, trend)" },
  ],
};

/** Görsel etiket — frontend render için kaynak tipini insan-okunabilir yapar. */
export const CONDITION_SOURCE_LABEL: Record<ConditionSource, string> = {
  mark: "Mark Resmi Kural",
  quanfina: "Quanfina Ek Filtre",
  mark_ekstra: "Mark Ekstra Kural",
  carr: "Carr Kuralı",
};
