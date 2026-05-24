/**
 * Para birimi ve yuzdeye iliskin format helper'lari.
 * KARAR #733 alt-paket (Paket 43, 25 May 2026, 24+ May 2026 mini-mühür v21.14
 * sonrasi): Bilgi Mimarisi İlke #4 (Tekrarsızlık) — DRY format kaynak.
 *
 * `web/app/(dashboard)/risk-yonetimi/page.tsx` icinde fmtUsd local idi;
 * `web/lib/math.ts` icinde fmtPct + fmtPrice mevcut. Bu dosya **dolar**
 * format icin DRY tek kaynak — 9+ sayfa kullanir (Risk Yonetimi, Dashboard,
 * Sinyaller, columns.ts paketleri).
 *
 * Quanfina ABD piyasasi -> USD ($) ana para birimi (CLAUDE.md Piyasa Bağlamı).
 * TR locale "en-US" — virgul ayirici (e.g. $100,000.50). Sn. Ferit Turkce
 * yasiyor ama ABD piyasasi formatini tercih eder.
 */

/**
 * Dolar formatla — locale ABD ("en-US"). Ondalik basamak sayisi opsiyonel.
 *
 * @param v Sayisal deger ($)
 * @param decimals Ondalik basamak (default 2)
 * @example fmtUsd(100000)   -> "$100,000.00"
 * @example fmtUsd(1450.30)  -> "$1,450.30"
 * @example fmtUsd(0.5, 4)   -> "$0.5000"
 * @example fmtUsd(null)     -> "—"
 */
export function fmtUsd(v: number | null | undefined, decimals: number = 2): string {
  if (v == null || Number.isNaN(v)) return "—";
  return `$${v.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })}`;
}

/**
 * Dolar formatla — kompakt (binler/milyonlar).
 *
 * @example fmtUsdCompact(1500)      -> "$1.5K"
 * @example fmtUsdCompact(2500000)   -> "$2.5M"
 */
export function fmtUsdCompact(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—";
  if (Math.abs(v) >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `$${(v / 1_000).toFixed(1)}K`;
  return fmtUsd(v);
}

/**
 * Yuzde formatla — isaret + ondalik. Pozitif degerlere "+" eklenir.
 *
 * @param v Yuzde degeri (orn. 5.2 = %5.2)
 * @param decimals Ondalik basamak (default 2)
 * @example fmtPct(5.2)        -> "+5.20%"
 * @example fmtPct(-3.1, 1)    -> "-3.1%"
 * @example fmtPct(null)       -> "—"
 */
export function fmtPctSigned(v: number | null | undefined, decimals: number = 2): string {
  if (v == null || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(decimals)}%`;
}
