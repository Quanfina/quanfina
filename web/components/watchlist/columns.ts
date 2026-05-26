import type { ColDef, ValueFormatterParams, CellClassParams } from "ag-grid-community";
import type { WatchlistRow } from "@/types/watchlist";
import { StatusBadge } from "./StatusBadge";
// ConsensusBadge import kaldirildi (KARAR ADAY 21 May 2026): Konsensus kavrami kaldirildi,
// her strateji ayri satir kanon. Komponent dosyasi (ConsensusBadge.tsx) silinmedi -
// Kural #18 pasif oge mantigi.
import { StrategyCellRenderer } from "./StrategyCellRenderer";
import { SetupCellRenderer } from "./SetupCellRenderer";
import { SymbolCellRenderer } from "./SymbolCellRenderer";
import { MarkBadgeCell } from "./MarkBadgeCell";
import { PivotBadgeCell } from "@/components/shared/PivotBadgeCell";
import { RsRatingBadge } from "@/components/shared/RsRatingBadge";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";
// KARAR #492 (20 May 2026 ~16:45): DRY hijyen - yerel MONO -> @/lib/grid-styles
// Sinyaller + Hisse Tarama paterniyle eşitleme. Davranış: fontSize 13px global ->
// 12px MONO_RIGHT (Sinyaller exact). Tüm tablolarda fiyat 12px tutarlı.
import { MONO_RIGHT as MONO } from "@/lib/grid-styles";
import { fmtUsd } from "@/lib/format-currency";

function fmtPrice(p: ValueFormatterParams<WatchlistRow>) {
  return fmtUsd(p.value as number | null);
}

function relativeDate(iso: string): string {
  const days = Math.floor((Date.now() - new Date(iso).getTime()) / 86_400_000);
  if (days === 0) return "Bugün";
  if (days === 1) return "Dün";
  return `${days} gün önce`;
}

function rsBackground(rs: number | null): string {
  if (rs == null) return "transparent";
  if (rs >= 90) return "color-mix(in srgb, var(--mtp-excellent) 35%, transparent)";
  if (rs >= 70) return "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)";
  if (rs >= 50) return "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)";
  return              "color-mix(in srgb, var(--mtp-danger)    18%, transparent)";
}

export const COL_DEFS: ColDef<WatchlistRow>[] = [
  {
    field: "symbol",
    headerName: "HİSSE",
    pinned: "left",
    width: 90,
    minWidth: 80,
    cellRenderer: SymbolCellRenderer,
  },
  {
    field: "strategy",
    headerName: "STRATEJİ",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "watchlist_status" },
    width: 120,
    minWidth: 110,
    cellRenderer: StrategyCellRenderer,
  },
  {
    field: "status",
    headerName: "STATÜ",
    width: 100,
    minWidth: 90,
    cellRenderer: StatusBadge,
  },
  {
    field: "price",
    headerName: "FİYAT",
    width: 90,
    minWidth: 80,
    valueFormatter: fmtPrice,
    cellStyle: MONO,
    headerTooltip: "Eklendiği zamanki fiyat",
  },
  // Paket 154 (26 May 2026): Watchlist canlı fiyat — paper trading sabah bakışı
  // useStockQuotes 60s refetch, yfinance + 5dk backend cache. MOCK fiyat değil.
  {
    field: "current_price",
    headerName: "CANLI $",
    width: 95,
    minWidth: 85,
    valueFormatter: fmtPrice,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => ({
      ...MONO,
      color: p.value == null ? "var(--muted-foreground)" : "var(--mtp-good, #4B9CD3)",
      fontWeight: 600,
    }),
    headerTooltip: "Anlık piyasa fiyatı (yfinance, 60s yenileme)",
  },
  {
    field: "change_pct_today",
    headerName: "GÜN %",
    width: 85,
    minWidth: 75,
    valueFormatter: (p) => {
      const v = p.value as number | null | undefined;
      if (v == null) return "—";
      return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
    },
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => {
      if (p.value == null) return { ...MONO, color: "var(--muted-foreground)" };
      return {
        ...MONO,
        color: p.value >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
        fontWeight: 600,
      };
    },
    headerTooltip: "Bugünkü yüzde değişim (son kapanış vs dünkü)",
  },
  {
    field: "setup_type",
    headerName: "SETUP",
    width: 160,
    minWidth: 140,
    cellRenderer: SetupCellRenderer,
  },
  // KARAR #733 alt-paket (Paket 98, 26 May 2026): RS Rating kategori rozet
  // Mark TLSMW Ch 3-5 / IBD canon — sadece sayı değil, kategori rozet
  // (LEADER/STRONG/AVERAGE/LAGGARD) ile Sn. Ferit anında "Top 20%" tarar.
  // Paket 158 (26 May 2026): document.createElement -> RsRatingBadge DRY (React 19 fix)
  {
    field: "rs_rating",
    headerName: "RS",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "rs_ibd" },
    width: 100,
    minWidth: 85,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => ({
      ...MONO,
      background: rsBackground(p.value ?? null),
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      paddingRight: "4px",
    }),
    cellRenderer: RsRatingBadge,
  },
  // KONSENSUS kolonu kaldirildi (KARAR ADAY 21 May 2026): her strateji ayri satir
  // kanon, konsensus sayim noise.
  {
    field: "added_date",
    headerName: "EKLENME",
    width: 110,
    minWidth: 100,
    valueFormatter: (p) => p.value ? relativeDate(p.value as string) : "—",
    cellStyle: { fontSize: 12, color: "var(--muted-foreground)" },
  },
  // KARAR ADAY #724 (24 May 2026): Mark Profili rozetleri (DRY MarkBadgeStrip)
  // Backend Migration 004-007 sonrasi otomatik canli. MOCK feed (_STOCK_MARK_SIGNALS).
  {
    headerName: "MARK PROFİLİ",
    minWidth: 240,
    flex: 1,
    sortable: false,
    filter: false,
    cellRenderer: MarkBadgeCell,
  },
  // KARAR #733 alt-paket (Paket 82): Pivot kolonu — Mark TLSMW Ch 10 (P81 Sinyaller paten)
  // Paket 153 (26 May 2026): document.createElement -> PivotBadgeCell React component
  // (React 19 crash fix — "Objects are not valid as a React child" — DRY shared).
  {
    headerName: "PIVOT",
    field: "pivot_status",
    width: 100,
    sortable: false,
    filter: false,
    cellRenderer: PivotBadgeCell,
  },
  {
    field: "note",
    headerName: "NOT",
    width: 140,
    minWidth: 100,
    valueFormatter: (p) => (p.value as string | null) ?? "",
    cellStyle: { fontSize: 12 },
  },
];

export const DEFAULT_COL_DEF: ColDef<WatchlistRow> = {
  sortable: true,
  resizable: true,
  filter: false,
  suppressMovable: false,
  autoHeight: false,
};
