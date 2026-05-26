"use client";

import type { PivotBreakoutStatus } from "@/hooks/use-pivot-breakout";

/**
 * Paket 153 (26 May 2026): Pivot status rozet — React component.
 *
 * Önceki kanon document.createElement döndürüyordu — React 19'da
 * "Objects are not valid as a React child" crash. AG Grid React mode
 * cellRenderer için saf React component gerekir.
 *
 * DRY: Journal columns.ts + Watchlist columns.ts ortak kullanımı.
 * Mark TLSMW Ch 10 — Pivot Buy Point disiplinli AL ✓ / Zayıf / Yakın / Altı.
 */
export interface PivotBadgeProps {
  data?: { pivot_status?: PivotBreakoutStatus | null } | null;
  value?: PivotBreakoutStatus | null;
}

export function PivotBadgeCell(p: PivotBadgeProps) {
  const status: PivotBreakoutStatus | null | undefined = p.data?.pivot_status ?? p.value;
  if (!status) return null;
  const meta =
    status === "CONFIRMED"
      ? { label: "AL ✓", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.15)" }
      : status === "WEAK"
      ? { label: "Zayıf ⚠️", color: "#F59E0B", bg: "rgba(245,158,11,0.15)" }
      : status === "NEAR_PIVOT"
      ? { label: "Yakın ⏳", color: "var(--mtp-good, #4B9CD3)", bg: "rgba(75,156,211,0.15)" }
      : { label: "Altı ○", color: "var(--mtp-danger)", bg: "rgba(220,53,69,0.10)" };
  const borderColor = status === "CONFIRMED" ? "var(--mtp-excellent)" : "currentColor";
  return (
    <span
      title={`Mark TLSMW Ch 10 — Pivot ${status}`}
      style={{
        fontSize: 10,
        fontWeight: 700,
        padding: "1px 6px",
        borderRadius: 4,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        background: meta.bg,
        color: meta.color,
        border: `1px solid ${borderColor}`,
      }}
    >
      {meta.label}
    </span>
  );
}
