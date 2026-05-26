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
  // KARAR #733 alt-paket (Paket 98, 26 May 2026): RS Rating kategori rozet
  // Mark TLSMW Ch 3-5 / IBD canon — sadece sayı değil, kategori rozet
  // (LEADER/STRONG/AVERAGE/LAGGARD) ile Sn. Ferit anında "Top 20%" tarar.
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
    cellRenderer: (p: { value?: number | null }) => {
      const rs = p.value;
      if (rs == null) {
        const el = document.createElement("span");
        el.textContent = "—";
        el.style.color = "var(--muted-foreground)";
        return el;
      }
      const container = document.createElement("span");
      container.style.cssText = "display:flex;align-items:center;gap:4px;width:100%;";

      const num = document.createElement("span");
      num.textContent = String(rs);
      num.style.cssText = "font-weight:600;font-variant-numeric:tabular-nums;";
      container.appendChild(num);

      // Kategori rozet (Mark/IBD canon)
      let meta: { label: string; color: string } | null = null;
      if (rs >= 80) meta = { label: "L", color: "var(--mtp-excellent)" };
      else if (rs >= 70) meta = { label: "S", color: "var(--mtp-good, #4B9CD3)" };
      else if (rs >= 50) meta = { label: "A", color: "#F59E0B" };
      else meta = { label: "↓", color: "var(--mtp-danger)" };

      if (meta) {
        const badge = document.createElement("span");
        badge.textContent = meta.label;
        badge.style.cssText = `font-size:9px;font-weight:700;padding:0 4px;border-radius:3px;background:${meta.color};color:#fff;margin-left:auto;`;
        const tooltip =
          rs >= 80 ? "IBD LEADER (Top 20%)"
          : rs >= 70 ? "STRONG"
          : rs >= 50 ? "AVERAGE"
          : "LAGGARD — Mark: uzak dur";
        badge.title = `${tooltip} (RS ${rs})`;
        container.appendChild(badge);
      }

      return container;
    },
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
