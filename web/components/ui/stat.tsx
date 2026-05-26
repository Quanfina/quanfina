/**
 * Paket 237 (27 May 2026): DRY Stat component — sayısal istatistik birimi.
 *
 * P156 Dashboard PortfolioSummaryCard + P228 Pazar Hazırlığı sayfası aynı
 * "label üst + değer alt + opsiyonel renk" patenini ayrı ayrı yazıyordu.
 * Bilgi Mimarisi İlke #4 (Tekrarsızlık/DRY) — tek kaynak.
 *
 * Vizyon İLKE #11 (Objektif Ayna Dili): Bloomberg Terminal stilinde
 * sayı + birim + tabular-nums. Mono font otomatik (JetBrains Mono).
 */

import { ReactNode } from "react";

export interface StatProps {
  label: string;
  value: string;
  color?: string;
  suffix?: ReactNode;
}

export function Stat({ label, value, color, suffix }: StatProps) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
        {label}
      </span>
      <span
        className="text-base font-semibold tabular-nums"
        style={{
          color,
          fontFamily: "var(--font-jetbrains-mono, monospace)",
        }}
      >
        {value}
        {suffix && <span className="ml-1 text-xs">{suffix}</span>}
      </span>
    </div>
  );
}
