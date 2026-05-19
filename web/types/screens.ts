/**
 * Sprint 4-bis.1b — 8 Ready Screen tipler
 * Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md (SCREENS tuple)
 *         api/db_helpers.py SCREENS_READY_8
 */

export type ScreenSlug =
  | "tpr_a"
  | "tpr_a_b"
  | "rpr_89_tpr_c"
  | "stage2_10p"
  | "stage2_below_10"
  | "top5_rpr"
  | "mom_10p"
  | "mom_below_10";

export interface ScreenMeta {
  slug: ScreenSlug;
  label: string;
  filter_summary: string;
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
  tpr_a: "TPR-Bazlı",
  tpr_a_b: "TPR-Bazlı",
  rpr_89_tpr_c: "TPR-Bazlı",
  stage2_10p: "Stage",
  stage2_below_10: "Stage",
  top5_rpr: "RPR-Bazlı",
  mom_10p: "Momentum",
  mom_below_10: "Momentum",
};
