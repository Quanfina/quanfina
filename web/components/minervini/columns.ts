import type { ColDef, ValueFormatterParams, CellClassParams, CellStyle } from "ag-grid-community";
import type { MinerviniStock, ListType } from "@/types/minervini";
import { LIST_LABELS } from "@/types/minervini";
import { GradeBadge } from "./GradeBadge";
import { ListTypeEditor } from "./ListTypeEditor";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";

const NUM: CellStyle = {
  fontFamily: "var(--font-jetbrains-mono, monospace)",
  fontVariantNumeric: "tabular-nums",
  textAlign: "right",
};

function fmtPrice(p: ValueFormatterParams<MinerviniStock>) {
  return p.value != null ? `$${(p.value as number).toFixed(2)}` : "-";
}
function fmtPct(p: ValueFormatterParams<MinerviniStock>) {
  const v = p.value as number;
  return v != null ? `${v >= 0 ? "+" : ""}${v.toFixed(2)}%` : "-";
}
function fmtPct1(p: ValueFormatterParams<MinerviniStock>) {
  const v = p.value as number;
  return v != null ? `${v.toFixed(1)}%` : "-";
}
function fmtInt(p: ValueFormatterParams<MinerviniStock>) {
  return p.value != null ? String(Math.round(p.value as number)) : "-";
}
function fmtCap(p: ValueFormatterParams<MinerviniStock>) {
  const v = p.value as number;
  if (v == null) return "-";
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}T`;
  return `$${v.toFixed(0)}B`;
}

export const COL_DEFS: ColDef<MinerviniStock>[] = [
  {
    field: "symbol",
    headerName: "HISSE",
    pinned: "left",
    width: 90,
    minWidth: 80,
    cellStyle: {
      fontWeight: 700,
      fontFamily: "var(--font-jetbrains-mono, monospace)",
    },
  },
  {
    field: "price",
    headerName: "FIYAT",
    width: 100,
    minWidth: 100,
    valueFormatter: fmtPrice,
    cellStyle: NUM,
  },
  {
    field: "change_pct",
    headerName: "DEĞİŞİM",
    width: 100,
    minWidth: 100,
    valueFormatter: fmtPct,
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => ({
      ...NUM,
      color: (p.value ?? 0) >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    }),
  },
  {
    field: "grade",
    headerName: "NOT",
    width: 80,
    minWidth: 80,
    cellRenderer: GradeBadge,
  },
  {
    field: "rs_ibd",
    headerName: "RS IBD",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "rs_ibd" },
    width: 90,
    minWidth: 90,
    valueFormatter: fmtInt,
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => {
      const hue = Math.round((Math.min(Math.max(p.value ?? 0, 0), 99) / 99) * 120);
      return { ...NUM, background: `hsla(${hue},65%,45%,0.18)` };
    },
  },
  {
    field: "ma200_slope",
    headerName: "MA200 EĞİM",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "ma200_slope" },
    width: 130,
    minWidth: 130,
    valueFormatter: (p) => (p.value != null ? (p.value as number).toFixed(2) : "-"),
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => ({
      ...NUM,
      color: (p.value ?? 0) >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    }),
  },
  {
    field: "pct_from_high",
    headerName: "52H MESAFE",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "high_52w" },
    width: 130,
    minWidth: 130,
    valueFormatter: fmtPct1,
    cellStyle: NUM,
  },
  {
    field: "eps_qoq",
    headerName: "EPS Q/Q",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "canslim" },
    width: 100,
    minWidth: 100,
    valueFormatter: fmtPct1,
    cellStyle: NUM,
  },
  {
    field: "confirmations",
    headerName: "ONAY",
    width: 80,
    minWidth: 80,
    valueFormatter: fmtInt,
    cellStyle: {
      ...NUM,
      background: "color-mix(in srgb, var(--mtp-excellent) 12%, transparent)",
    },
  },
  {
    field: "violations",
    headerName: "İHLAL",
    width: 80,
    minWidth: 80,
    valueFormatter: fmtInt,
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => ({
      ...NUM,
      background:
        (p.value ?? 0) > 0
          ? "color-mix(in srgb, var(--mtp-danger) 12%, transparent)"
          : "transparent",
    }),
  },
  // visible extra columns
  {
    field: "market_cap",
    headerName: "PİYASA DEĞ",
    width: 130,
    minWidth: 130,
    valueFormatter: fmtCap,
    cellStyle: NUM,
  },
  // list_type editor (visible, editable)
  {
    field: "list_type",
    headerName: "LİSTE",
    width: 110,
    minWidth: 110,
    editable: true,
    singleClickEdit: true,
    cellEditor: ListTypeEditor,
    valueFormatter: (p) => LIST_LABELS[p.value as ListType] ?? (p.value as string),
    cellStyle: (p: CellClassParams<MinerviniStock, string>) => {
      const bg: Record<string, string> = {
        buy:    "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)",
        focus:  "color-mix(in srgb, var(--mtp-good)      14%, transparent)",
        on_deck:"color-mix(in srgb, var(--mtp-neutral)   14%, transparent)",
        watch:  "transparent",
      };
      return { background: bg[p.value ?? ""] ?? "transparent" };
    },
  },
  // hidden metadata
  { field: "company", hide: true },
  { field: "sector", hide: true },
  { field: "high52", hide: true },
];

export const DEFAULT_COL_DEF: ColDef<MinerviniStock> = {
  sortable: true,
  resizable: true,
  filter: false,
  suppressMovable: false,
  autoHeight: false,
};
