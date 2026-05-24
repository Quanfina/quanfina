"use client";

import type { ICellRendererParams } from "ag-grid-community";
import type { Trade } from "@/types/trade";
import { computeRMultiple, formatR } from "@/lib/r-multiple";

/**
 * KARAR ADAY #734 (24 May 2026) — R-Multiple AG Grid cell renderer.
 * Mark RBA disiplini: plan_stop + entry + exit → R-Multiple görsel.
 */
export function RMultipleCell(p: ICellRendererParams<Trade>) {
  const t = p.data;
  if (!t || t.plan_stop == null || t.exit_price == null) {
    return <span style={{ color: "var(--muted-foreground)" }}>—</span>;
  }
  const result = computeRMultiple(t.entry_price, t.plan_stop, t.exit_price, t.shares);
  if (!result) {
    return <span style={{ color: "var(--muted-foreground)" }}>—</span>;
  }
  return (
    <span
      title={result.markSays}
      className="font-mono font-semibold tabular-nums"
      style={{ color: result.color, fontSize: 12 }}
    >
      {formatR(result.r)}
    </span>
  );
}
