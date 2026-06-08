/**
 * AG Grid ortak hücre stilleri (DRY) — KARAR #479 helper soyutlama.
 *
 * Sinyaller, Hisse Tarama, Watchlist, Journal sayfalarında tablo
 * stil hizalaması için tek kaynak. Yerel `const MONO = {...}` kopyalama
 * yerine bu modülü import et.
 *
 * JetBrains Mono fontu `web/app/layout.tsx`'te `--font-jetbrains-mono`
 * CSS değişkenine bağlandığı için her sayfada otomatik geçerli.
 *
 * Disiplin: yeni stil ihtiyaçları burada eklenmeli, sayfa-içi
 * kopyalama tekrarı yasak (Kural #18 Pasif Öğe + İlke #4 Tekrarsızlık).
 */

import type { CellStyle } from "ag-grid-community";

/** Numeric sütunlar için monospace + tabular-nums (sayı hizalama). */
export const MONO: CellStyle = {
  fontFamily: "var(--font-jetbrains-mono, monospace)",
  fontVariantNumeric: "tabular-nums",
  fontSize: "12px",
};

/** Sağ hizalı numeric sütunlar (Fiyat, R/R, Volume vb.). */
export const MONO_RIGHT: CellStyle = {
  ...MONO,
  textAlign: "right",
};

/** Ortalanmış numeric sütunlar (rozet/grade kolonları). */
export const MONO_CENTER: CellStyle = {
  ...MONO,
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
};

/** Sembol sütunu — semibold sembol görünümü + monospace. */
export const SYMBOL_CELL: CellStyle = {
  ...MONO,
  fontWeight: 600,
  fontSize: "13px",
};

/**
 * RS Rating arka plan bandı — color-mix MTP renk sistemi (DRY tek kaynak).
 *
 * P420 (31 May 2026): Önceki kanon İzleme Listesi (watchlist/columns.ts) ve
 * Tarama (screens/page.tsx) AYNI helper'ı ayrı ayrı kopyalıyordu — biri
 * değişirse RS bandı sayfalar arası sessizce ayrışırdı. Workflow tasarım
 * denetimi bu çiftlemeyi yakaladı → tek kaynağa taşındı.
 *
 * Eşikler IBD canon: ≥90 lider yoğun yeşil, ≥70 güçlü açık yeşil,
 * ≥50 ortalama nötr, <50 zayıf kırmızı (Mark "Leaders First" disiplini).
 */
export function rsBackground(rs: number | null | undefined): string {
  if (rs == null) return "transparent";
  if (rs >= 90) return "color-mix(in srgb, var(--mtp-excellent) 35%, transparent)";
  if (rs >= 70) return "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)";
  if (rs >= 50) return "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)";
  return              "color-mix(in srgb, var(--mtp-danger)    18%, transparent)";
}
