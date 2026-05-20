import type { ColDef, ValueFormatterParams, CellStyle, CellClassParams } from "ag-grid-community";
import type { Trade } from "@/types/trade";
import { SETUP_LABELS } from "@/types/trade";
import { GradeBadge } from "./GradeBadge";
import { TradeStatusBadge } from "./TradeStatusBadge";
import { SymbolCellRenderer } from "@/components/watchlist/SymbolCellRenderer";
import { formatDateTR } from "@/lib/format-date";

const MONO: CellStyle = {
  fontFamily: "var(--font-jetbrains-mono, monospace)",
  fontVariantNumeric: "tabular-nums",
  textAlign: "right",
};

// KARAR #471 + #472 (20 May 2026): TR tarih formatı — ortak helper
// (eski 2-digit year "26" karışıklığı giderildi, full DD.MM.YYYY).
function fmtDate(p: ValueFormatterParams<Trade>): string {
  return formatDateTR(p.value as string | null);
}

function fmtPrice(p: ValueFormatterParams<Trade>): string {
  return p.value != null ? `$${(p.value as number).toFixed(2)}` : "—";
}

function plStyle(p: CellClassParams<Trade, number>): CellStyle {
  if (p.value == null) return { color: "var(--muted-foreground)", ...MONO };
  return {
    ...MONO,
    color: p.value >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    fontWeight: 600,
  };
}

function fmtPLDollar(p: ValueFormatterParams<Trade>): string {
  const v = p.value as number | null;
  if (v == null) return "—";
  const abs = Math.abs(v).toFixed(2);
  return v >= 0 ? `+$${abs}` : `-$${abs}`;
}

function fmtPLPct(p: ValueFormatterParams<Trade>): string {
  const v = p.value as number | null;
  if (v == null) return "—";
  return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
}


export const TRADE_COL_DEFS: ColDef<Trade>[] = [
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
    width: 110,
    minWidth: 90,
    valueFormatter: (p) =>
      p.value === "minervini" ? "Minervini" : p.value === "carr" ? "Carr" : String(p.value ?? ""),
    cellStyle: { fontSize: 12 },
  },
  {
    field: "setup_type",
    headerName: "SETUP",
    width: 130,
    minWidth: 110,
    valueFormatter: (p) => SETUP_LABELS[p.value as string] ?? String(p.value ?? ""),
    cellStyle: { fontSize: 12 },
  },
  {
    field: "entry_date",
    headerName: "GİRİŞ TARİH",
    width: 110,
    minWidth: 90,
    valueFormatter: fmtDate,
    cellStyle: { fontSize: 12, color: "var(--muted-foreground)" },
  },
  {
    field: "entry_price",
    headerName: "GİRİŞ $",
    width: 95,
    minWidth: 80,
    valueFormatter: fmtPrice,
    cellStyle: MONO,
  },
  {
    field: "exit_date",
    headerName: "ÇIKIŞ TARİH",
    width: 110,
    minWidth: 90,
    valueFormatter: fmtDate,
    cellStyle: { fontSize: 12, color: "var(--muted-foreground)" },
  },
  {
    field: "exit_price",
    headerName: "ÇIKIŞ $",
    width: 95,
    minWidth: 80,
    valueFormatter: fmtPrice,
    cellStyle: MONO,
  },
  {
    field: "pl_dollar",
    headerName: "P/L $",
    width: 100,
    minWidth: 85,
    valueFormatter: fmtPLDollar,
    cellStyle: plStyle as (p: CellClassParams) => CellStyle,
  },
  {
    field: "pl_pct",
    headerName: "P/L %",
    width: 85,
    minWidth: 70,
    valueFormatter: fmtPLPct,
    cellStyle: plStyle as (p: CellClassParams) => CellStyle,
  },
  {
    field: "grade",
    headerName: "GRADE",
    width: 80,
    minWidth: 70,
    cellRenderer: GradeBadge,
    cellStyle: { display: "flex", alignItems: "center" },
  },
  {
    field: "status",
    headerName: "STATÜ",
    width: 90,
    minWidth: 80,
    cellRenderer: TradeStatusBadge,
    cellStyle: { display: "flex", alignItems: "center" },
  },
];

export const TRADE_DEFAULT_COL_DEF: ColDef<Trade> = {
  sortable: true,
  resizable: true,
  filter: false,
  suppressMovable: false,
};
