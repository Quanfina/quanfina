"use client";

import type { ICellRendererParams } from "ag-grid-community";
import type { Signal } from "@/types/signal";

/**
 * Paket 158 (26 May 2026): Sinyaller R/R hücresi (RS + Climax + R/R birleşik).
 *
 * Önceki kanon (P113) document.createElement DOM döndürüyordu — React 19'da
 * "Objects are not valid as a React child" crash önleme.
 *
 * Görsel:
 * - 🔥 Climax Top ikon (varsa)
 * - RS kategori rozet (L/S/A/↓)
 * - R/R sayı (renkli: >=3 yeşil, >=2 mavi, >=1 nötr, <1 kırmızı)
 */
export function SignalRREnrichedCell(p: ICellRendererParams<Signal>) {
  const v = p.value as number | null | undefined;
  const row = p.data;
  if (v == null) return <span style={{ color: "var(--muted-foreground)" }}>—</span>;

  const isClimaxTop = row?.mark_signals?.climax_category === "CLIMAX_TOP";
  const rs = row?.rs_rating;
  let rsMeta: { label: string; color: string; tip: string } | null = null;
  if (rs != null) {
    const rsR = Math.round(rs);
    if (rsR >= 80) rsMeta = { label: "L", color: "var(--mtp-excellent)", tip: "IBD LEADER" };
    else if (rsR >= 70) rsMeta = { label: "S", color: "var(--mtp-good, #4B9CD3)", tip: "STRONG" };
    else if (rsR >= 50) rsMeta = { label: "A", color: "#F59E0B", tip: "AVERAGE" };
    else rsMeta = { label: "↓", color: "var(--mtp-danger)", tip: "LAGGARD" };
  }

  const rrColor =
    v >= 3 ? "var(--mtp-excellent)" :
    v >= 2 ? "var(--mtp-good)" :
    v >= 1 ? "var(--mtp-neutral)" :
             "var(--mtp-danger)";

  return (
    <span style={{
      display: "flex",
      alignItems: "center",
      gap: 4,
      width: "100%",
      justifyContent: "flex-end",
    }}>
      {isClimaxTop && (
        <span
          style={{ fontSize: 10, lineHeight: 1 }}
          title="Climax Top — Mark: pozisyon azalt/kapat"
        >
          🔥
        </span>
      )}
      {rsMeta && rs != null && (
        <span
          title={`${rsMeta.tip} (RS ${Math.round(rs)})`}
          style={{
            fontSize: 9,
            fontWeight: 700,
            padding: "0 4px",
            borderRadius: 3,
            background: rsMeta.color,
            color: "#fff",
          }}
        >
          {rsMeta.label}
        </span>
      )}
      <span
        title={`Risk/Reward = (hedef-fiyat) / (fiyat-stop) = ${v.toFixed(2)}`}
        style={{
          fontWeight: 600,
          fontVariantNumeric: "tabular-nums",
          color: rrColor,
          fontFamily: "var(--font-jetbrains-mono, monospace)",
          fontSize: 12,
        }}
      >
        {v.toFixed(2)}
      </span>
    </span>
  );
}
