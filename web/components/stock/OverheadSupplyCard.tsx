"use client";

import { Layers2, AlertTriangle, Activity, CheckCircle2 } from "lucide-react";
import { useOverheadSupply, type OverheadSupplyCategory } from "@/hooks/use-overhead-supply";
import { fmtUsd } from "@/lib/format-currency";

/**
 * KARAR #733 alt-paket (Paket 78): Overhead Supply widget /hisse/[symbol].
 * Mark TLSMW Ch 10 — "eski sahipler kayıp kapansın satışı = direnç".
 */

interface Meta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const META: Record<OverheadSupplyCategory, Meta> = {
  HEAVY: {
    label: "Ağır Direnç",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.10)",
    icon: <AlertTriangle size={14} />,
  },
  MODERATE: {
    label: "Orta Direnç",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <Activity size={14} />,
  },
  NONE: {
    label: "Temiz",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.08)",
    icon: <CheckCircle2 size={14} />,
  },
};

export function OverheadSupplyCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useOverheadSupply(symbol);
  if (isLoading || isError || !data || !data.category) return null;

  const meta = META[data.category];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      <div className="flex items-center gap-2">
        <Layers2 size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Overhead Supply
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 10)
          </span>
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: meta.color, color: "#fff" }}
        >
          {meta.icon}
          {meta.label}
        </span>
      </div>

      <p
        className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {data.overhead_price != null && (
        <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <div className="flex flex-col">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
              Tepe (60g)
            </span>
            <span
              className="font-mono font-semibold tabular-nums"
              style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            >
              {fmtUsd(data.overhead_price)}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
              Yakınlık
            </span>
            <span
              className="font-mono font-semibold tabular-nums"
              style={{ color: meta.color }}
            >
              {data.proximity_pct != null ? `+${data.proximity_pct.toFixed(2)}%` : "—"}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
