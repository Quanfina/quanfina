/**
 * Sprint 4-bis Screen tipler
 * Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md (SCREENS tuple)
 *         api/db_helpers.py SCREENS_READY_9 + SCREENS_PARSE_7 + SCREENS_DIFF_6
 *
 * KARAR #461 (19 May 2026): tight_low_volume Deferred -> Ready (pre-compute,
 * scanner.py tight_low_vol_pass BOOLEAN yazar). Kategori sayim: 9+7+6+0.
 */

export type ScreenSlug =
  // Sprint 4-bis.1b — Ready (9, saf SQL, KARAR #461 ile tight_low_volume dahil)
  | "tpr_a"
  | "tpr_a_b"
  | "rpr_89_tpr_c"
  | "stage2_10p"
  | "stage2_below_10"
  | "top5_rpr"
  | "mom_10p"
  | "mom_below_10"
  | "tight_low_volume"  // KARAR #461 Sprint 4-bis.4 — pre-compute, Ready'ye tasindi
  // Sprint 4-bis.2 — Parse (7, confirmations/violations text-parse)
  | "stage2_loose_10p"
  | "stage2_loose_below"
  | "stage2_vloose_10p"
  | "stage2_vloose_below"
  | "buy_risk_green"
  | "momentum_5x_rpr_70"
  | "mom_qualifier"
  // Sprint 4-bis.3 — Scan-Diff (6, Self-JOIN onceki scan karsilastirma)
  | "tpr_moving_up"
  | "tpr_jump_2"
  | "tpr_d_to_b"
  | "new_top5_rpr"
  | "new_7d_rpr_10p"
  | "new_7d_rpr_below";

// Sprint 4-bis.4 KARAR #461: "deferred" kategorisi boşaldı, 30 gün sonra
// Kural #18 pasif öğe çıkarma adayı (silinebilir).
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
}

/** Kategoriler — Notebook_C1 SCREENS tuple'dan */
export const SCREEN_CATEGORIES: Record<ScreenSlug, string> = {
  // Ready (8)
  tpr_a: "TPR-Bazlı",
  tpr_a_b: "TPR-Bazlı",
  rpr_89_tpr_c: "TPR-Bazlı",
  stage2_10p: "Stage",
  stage2_below_10: "Stage",
  top5_rpr: "RPR-Bazlı",
  mom_10p: "Momentum",
  mom_below_10: "Momentum",
  tight_low_volume: "Pattern (VCP)",  // KARAR #461 — Ready'ye taşındı
  // Parse (7)
  stage2_loose_10p: "Stage (Loose)",
  stage2_loose_below: "Stage (Loose)",
  stage2_vloose_10p: "Stage (Very Loose)",
  stage2_vloose_below: "Stage (Very Loose)",
  buy_risk_green: "Pattern",
  momentum_5x_rpr_70: "Momentum 5x",
  mom_qualifier: "Momentum",
  // Scan-Diff (6)
  tpr_moving_up: "TPR-Bazli (Diff)",
  tpr_jump_2: "TPR-Bazli (Diff)",
  tpr_d_to_b: "TPR-Bazli (Diff)",
  new_top5_rpr: "RPR-Bazli (Diff)",
  new_7d_rpr_10p: "RPR-Bazli (Diff)",
  new_7d_rpr_below: "RPR-Bazli (Diff)",
};
