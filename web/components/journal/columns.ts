import type { ColDef, ValueFormatterParams, CellStyle, CellClassParams } from "ag-grid-community";
import type { Trade } from "@/types/trade";
import { SETUP_LABELS } from "@/types/trade";
import { GradeBadge } from "./GradeBadge";
import { TradeStatusBadge } from "./TradeStatusBadge";
import { SymbolCellRenderer } from "@/components/watchlist/SymbolCellRenderer";
import { formatDateTR } from "@/lib/format-date";
import { fmtUsd, fmtPctSigned } from "@/lib/format-currency";
import { RMultipleCell } from "./RMultipleCell";
import { PivotBadgeCell } from "@/components/shared/PivotBadgeCell";
// KARAR #492 (20 May 2026 ~16:45): DRY hijyen - yerel MONO -> @/lib/grid-styles
// 4 sayfa tek noktada toplandi (signals/screens/watchlist/journal). fontSize 12px
// MONO_RIGHT global tutarlilik (Sinyaller pateniyle exact match).
import { MONO_RIGHT as MONO } from "@/lib/grid-styles";

// KARAR #471 + #472 (20 May 2026): TR tarih formatı — ortak helper
// (eski 2-digit year "26" karışıklığı giderildi, full DD.MM.YYYY).
function fmtDate(p: ValueFormatterParams<Trade>): string {
  return formatDateTR(p.value as string | null);
}

function fmtPrice(p: ValueFormatterParams<Trade>): string {
  return fmtUsd(p.value as number | null);
}

function plStyle(p: CellClassParams<Trade, number>): CellStyle {
  if (p.value == null) return { color: "var(--muted-foreground)", ...MONO };
  return {
    ...MONO,
    color: p.value >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    fontWeight: 600,
  };
}

// KARAR #733 alt-paket (Paket 45): fmtUsd default sign yok, P/L icin
// pozitif "+" prefix gerek -> sign manuel ekleniyor + fmtUsd kalan format.
function fmtPLDollar(p: ValueFormatterParams<Trade>): string {
  const v = p.value as number | null;
  if (v == null) return "—";
  return (v >= 0 ? "+" : "-") + fmtUsd(Math.abs(v));
}

function fmtPLPct(p: ValueFormatterParams<Trade>): string {
  return fmtPctSigned(p.value as number | null);
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
  // Paket 151 (26 May 2026): Paper trading canlı P&L — açık trade'lerde
  // yfinance current_price + unrealized P&L. Closed trade'ler için boş ("—").
  {
    field: "current_price",
    headerName: "CANLI $",
    width: 95,
    minWidth: 80,
    valueFormatter: (p) => {
      const v = p.value as number | null | undefined;
      if (v == null) return "—";
      return fmtUsd(v);
    },
    cellStyle: (p: CellClassParams<Trade, number>): CellStyle => {
      if (p.value == null) return { color: "var(--muted-foreground)", ...MONO };
      // Yeşil tonlu vurgu — canlı piyasa
      return { ...MONO, color: "var(--mtp-good, #4B9CD3)", fontWeight: 600 };
    },
  },
  {
    field: "unrealized_pl_dollar",
    headerName: "P/L $ (CANLI)",
    width: 115,
    minWidth: 95,
    valueFormatter: (p) => {
      const v = p.value as number | null | undefined;
      if (v == null) return "—";
      return (v >= 0 ? "+" : "-") + fmtUsd(Math.abs(v));
    },
    cellStyle: plStyle as (p: CellClassParams) => CellStyle,
  },
  {
    field: "unrealized_pl_pct",
    headerName: "P/L % (CANLI)",
    width: 100,
    minWidth: 85,
    valueFormatter: (p) => {
      const v = p.value as number | null | undefined;
      if (v == null) return "—";
      return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
    },
    cellStyle: plStyle as (p: CellClassParams) => CellStyle,
  },
  // KARAR ADAY #734 (24 May 2026): R-Multiple sutunu — Mark RBA disiplinini guclendir.
  // Plan_stop + entry + exit + shares ile hesaplanir; plan_stop yoksa "—".
  // NOT: sortable false (hesaplanan field, AG Grid sort fonksiyonu kullanmiyoruz).
  {
    headerName: "R",
    width: 80,
    minWidth: 70,
    sortable: false,
    filter: false,
    cellRenderer: RMultipleCell,
    cellStyle: { display: "flex", alignItems: "center", justifyContent: "flex-end" },
  },
  // KARAR #733 alt-paket (Paket 84, 26 May 2026): Pivot kolonu — Mark TLSMW Ch 10
  // (P81 Sinyaller + P82 Watchlist + P83 Tarama paten birebir). Trade enrichment
  // _enrich_trade_with_mark_signals -> _compute_signal_pivot_status (entry_price baz).
  // Trade gecmis kayit oldugundan pivot status "snapshot" niteliginde (anlik degil).
  // Paket 153 (26 May 2026): document.createElement -> PivotBadgeCell React component
  // (React 19 crash fix — "Objects are not valid as a React child").
  {
    headerName: "PIVOT",
    field: "pivot_status",
    width: 100,
    minWidth: 90,
    sortable: false,
    filter: false,
    cellRenderer: PivotBadgeCell,
  },
  // P477 (#976, 16 Haz 2026): Satış Gücü — açık pozisyon Mark satış sinyalleri (entry-aware
  // Hard Stop %10 + market sinyaller). HOLD/yok → "—" (temiz); WATCH/REDUCE/SELL → kategori +
  // skor renkli. Backend enrichment (sadece open trade). field kullanılır (valueGetter DEĞİL — clean-room #4).
  {
    headerName: "SATIŞ",
    field: "sell_strength",
    width: 100,
    minWidth: 85,
    sortable: false,
    filter: false,
    valueFormatter: (p) => {
      const ss = p.value as Trade["sell_strength"];
      if (!ss?.category || ss.category === "HOLD") return "—";
      const lbl: Record<string, string> = { SELL: "SAT", REDUCE: "AZALT", WATCH: "İZLE" };
      return `${lbl[ss.category] ?? ss.category} ${ss.score}`;
    },
    cellStyle: (p: CellClassParams<Trade>): CellStyle => {
      const cat = p.data?.sell_strength?.category;
      const color =
        cat === "SELL" ? "var(--mtp-danger)" :
        cat === "REDUCE" ? "#F59E0B" :
        cat === "WATCH" ? "var(--mtp-neutral)" :
        "var(--muted-foreground)";
      return { ...MONO, color, fontWeight: cat && cat !== "HOLD" ? 600 : 400 };
    },
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
