import type { ColDef, ValueFormatterParams, CellClassParams, CellStyle } from "ag-grid-community";
import type { WatchlistRow } from "@/types/watchlist";
import { StatusBadge } from "./StatusBadge";
// ConsensusBadge import kaldirildi (KARAR ADAY 21 May 2026): Konsensus kavrami kaldirildi,
// her strateji ayri satir kanon. Komponent dosyasi (ConsensusBadge.tsx) silinmedi -
// Kural #18 pasif oge mantigi.
import { StrategyCellRenderer } from "./StrategyCellRenderer";
import { SetupCellRenderer } from "./SetupCellRenderer";
import { SymbolCellRenderer } from "./SymbolCellRenderer";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";
// KARAR #492 (20 May 2026 ~16:45): DRY hijyen - yerel MONO -> @/lib/grid-styles
// Sinyaller + Hisse Tarama paterniyle eşitleme. Davranış: fontSize 13px global ->
// 12px MONO_RIGHT (Sinyaller exact). Tüm tablolarda fiyat 12px tutarlı.
import { MONO_RIGHT as MONO } from "@/lib/grid-styles";

function fmtPrice(p: ValueFormatterParams<WatchlistRow>) {
  return p.value != null ? `$${(p.value as number).toFixed(2)}` : "—";
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
    width: 100,
    minWidth: 90,
    valueFormatter: fmtPrice,
    cellStyle: MONO,
  },
  {
    field: "setup_type",
    headerName: "SETUP",
    width: 160,
    minWidth: 140,
    cellRenderer: SetupCellRenderer,
  },
  {
    field: "rs_rating",
    headerName: "RS",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "rs_ibd" },
    width: 80,
    minWidth: 70,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => ({
      ...MONO,
      background: rsBackground(p.value ?? null),
    }),
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
