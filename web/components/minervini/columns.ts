import type { ColDef, ValueFormatterParams, CellClassParams, CellStyle } from "ag-grid-community";
import type { MinerviniStock } from "@/types/minervini";

const NUM: CellStyle = {
  fontFamily: "var(--font-jetbrains-mono, monospace)",
  fontVariantNumeric: "tabular-nums",
  textAlign: "right",
};

const GRADE_COLORS: Record<string, { bg: string; color: string }> = {
  A: { bg: "var(--mtp-excellent)", color: "#fff" },
  B: { bg: "var(--mtp-neutral)", color: "#fff" },
  C: { bg: "var(--mtp-waiting)", color: "#222" },
  D: { bg: "var(--mtp-danger)", color: "#fff" },
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
    cellStyle: {
      fontWeight: 700,
      fontFamily: "var(--font-jetbrains-mono, monospace)",
    },
  },
  {
    field: "price",
    headerName: "FIYAT",
    width: 95,
    valueFormatter: fmtPrice,
    cellStyle: NUM,
  },
  {
    field: "change_pct",
    headerName: "DEĞİŞİM",
    width: 98,
    valueFormatter: fmtPct,
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => ({
      ...NUM,
      color: (p.value ?? 0) >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    }),
  },
  {
    field: "grade",
    headerName: "NOT",
    width: 68,
    cellRenderer: (p: { value: string }) => {
      const c = GRADE_COLORS[p.value] ?? { bg: "transparent", color: "inherit" };
      return `<span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:20px;border-radius:4px;background:${c.bg};color:${c.color};font-size:11px;font-weight:700;font-family:var(--font-jetbrains-mono,monospace)">${p.value}</span>`;
    },
  },
  {
    field: "rs_ibd",
    headerName: "RS IBD",
    width: 85,
    valueFormatter: fmtInt,
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => {
      const hue = Math.round((Math.min(Math.max(p.value ?? 0, 0), 99) / 99) * 120);
      return { ...NUM, background: `hsla(${hue},65%,45%,0.18)` };
    },
  },
  {
    field: "ma200_slope",
    headerName: "MA200 EĞİM",
    width: 112,
    valueFormatter: (p) => (p.value != null ? (p.value as number).toFixed(2) : "-"),
    cellStyle: (p: CellClassParams<MinerviniStock, number>) => ({
      ...NUM,
      color: (p.value ?? 0) >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
    }),
  },
  {
    field: "pct_from_high",
    headerName: "52H MESAFE",
    width: 108,
    valueFormatter: fmtPct1,
    cellStyle: NUM,
  },
  {
    field: "eps_qoq",
    headerName: "EPS Q/Q",
    width: 92,
    valueFormatter: fmtPct1,
    cellStyle: NUM,
  },
  {
    field: "confirmations",
    headerName: "ONAY",
    width: 72,
    valueFormatter: fmtInt,
    cellStyle: {
      ...NUM,
      background: "color-mix(in srgb, var(--mtp-excellent) 12%, transparent)",
    },
  },
  {
    field: "violations",
    headerName: "İHLAL",
    width: 72,
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
    width: 108,
    valueFormatter: fmtCap,
    cellStyle: NUM,
  },
  // hidden metadata
  { field: "list_type", hide: true },
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
