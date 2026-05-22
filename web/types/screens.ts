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
}

/**
 * Kategoriler — Sprint 4-bis.7 Adim Adim Yeniden Ekleme (22 May 2026)
 * Tum 25 eski slug -> kategori esleme notebook/Tarama_Listeleri_Arsiv.md'de.
 *
 * Yeniden ekleme: bu objeye slug + kategori ekle, dropdown'da gozukur.
 * Backend zaten /api/screens + /api/screens/{slug} canli (db_helpers.SCREENS_READY_9).
 *
 * Adim 1 (22 May 2026): stage2_10p — Minervini Trend Template (Mark resmi kuralı 8 kosul).
 * Notebook_A satir 846 kanon: passed=1 = Trend Template PASS.
 * Web Claude 5-Kaynak sentez: "Must-Have Setup #1 ZATEN CANLI."
 */
export const SCREEN_CATEGORIES: Record<string, string> = {
  stage2_10p: "Trend Template",
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
export type ConditionSource = "mark" | "quanfina";

export interface ScreenCondition {
  source: ConditionSource;
  text: string;
}

export const SCREEN_CONDITIONS: Record<string, ScreenCondition[]> = {
  stage2_10p: [
    { source: "mark", text: "Fiyat > 150 günlük hareketli ortalama (150DMA)" },
    { source: "mark", text: "Fiyat > 200 günlük hareketli ortalama (200DMA)" },
    { source: "mark", text: "150DMA > 200DMA (uzun vadeli yükseliş yapısı)" },
    { source: "mark", text: "200DMA en az 1 aydır yukarı yönlü (trending up)" },
    { source: "mark", text: "50DMA > 150DMA > 200DMA (sıralı piramit dizilimi)" },
    { source: "mark", text: "Fiyat > 50 günlük hareketli ortalama (50DMA)" },
    { source: "mark", text: "Fiyat 52 haftalık dipten en az %25 yukarıda" },
    { source: "mark", text: "Fiyat 52 haftalık zirveye en fazla %25 mesafede" },
    { source: "mark", text: "Relative Strength (RS) sıralaması ≥ 70 (IBD)" },
    { source: "quanfina", text: "Fiyat ≥ $10 (mikro-cap eleme)" },
    // 22 May 2026 — Sn. Ferit yakaladı: evren daraltma filtresi UI'de eksikti.
    // scanner.py Finviz URL: sh_avgvol_o500 — backend zaten uyguluyor, UI şeffaflık fix.
    { source: "quanfina", text: "Ortalama hacim ≥ 500.000 (likidite eleme)" },
  ],
};

/** Görsel etiket — frontend render için kaynak tipini insan-okunabilir yapar. */
export const CONDITION_SOURCE_LABEL: Record<ConditionSource, string> = {
  mark: "Mark Resmi Kural",
  quanfina: "Quanfina Ek Filtre",
};
