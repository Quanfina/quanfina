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
  // KARAR #467 — Power Play (HTF) Mark canon
  power_play_pass?: boolean | null;
}

/**
 * Kategoriler — ARSIV MODU (22 May 2026)
 * Tum 25 slug -> kategori esleme notebook/Tarama_Listeleri_Arsiv.md'de.
 * Yeniden ekleme: bu objeye slug + kategori ekle, dropdown'da gozukur.
 */
export const SCREEN_CATEGORIES: Record<string, string> = {};
