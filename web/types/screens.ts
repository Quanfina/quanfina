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
export type ConditionSource = "mark" | "quanfina" | "mark_ekstra";

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
};
