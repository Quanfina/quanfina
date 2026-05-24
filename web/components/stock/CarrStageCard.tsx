"use client";

import { TrendingUp, AlertCircle, XCircle, Clock } from "lucide-react";
import { useCarrStage, type CarrStage } from "@/hooks/use-carr-stage";

/**
 * KARAR #733 alt-paket (Paket 32): Carr Stage detay paneli (/hisse/[symbol]).
 *
 * Stan Weinstein 4-Stage gösterimi:
 * - Stage 1 (Basing): ⏳ hazırlık
 * - Stage 2 (Advancing): 📈 Mark+Carr alım fazı
 * - Stage 3 (Topping): ⚠️ çıkış hazırlık
 * - Stage 4 (Declining): ⛔ UZAK DUR
 */

interface StageMeta {
  emoji: string;
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const STAGE_META: Record<1 | 2 | 3 | 4, StageMeta> = {
  1: {
    emoji: "⏳",
    label: "Basing",
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75, 156, 211, 0.10)",
    icon: <Clock size={16} />,
  },
  2: {
    emoji: "📈",
    label: "Advancing",
    color: "var(--mtp-excellent)",
    bg: "rgba(40, 167, 69, 0.10)",
    icon: <TrendingUp size={16} />,
  },
  3: {
    emoji: "⚠️",
    label: "Topping",
    color: "#F59E0B",
    bg: "rgba(245, 158, 11, 0.10)",
    icon: <AlertCircle size={16} />,
  },
  4: {
    emoji: "⛔",
    label: "Declining",
    color: "var(--mtp-danger)",
    bg: "rgba(220, 53, 69, 0.10)",
    icon: <XCircle size={16} />,
  },
};

function fmtPct(v: number | null, decimals: number = 2): string {
  if (v == null) return "—";
  return `${v > 0 ? "+" : ""}${v.toFixed(decimals)}%`;
}

function fmtPrice(v: number | null): string {
  return v == null ? "—" : `$${v.toFixed(2)}`;
}

export function CarrStageCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useCarrStage(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Carr Stage yükleniyor...
      </div>
    );
  }

  if (isError || !data) return null;
  if (!data.stage) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        <span className="font-semibold">Carr Stage: </span>
        {data.stage_label} — {data.mark_says}
      </div>
    );
  }

  const meta = STAGE_META[data.stage as 1 | 2 | 3 | 4];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <span aria-hidden="true" className="text-xl leading-none">
          {meta.emoji}
        </span>
        <div className="flex flex-col flex-1 min-w-0">
          <h3 className="text-xs font-semibold flex items-center gap-1.5">
            <span style={{ color: meta.color }}>{meta.icon}</span>
            Carr Stage Analizi
            <span className="text-[10px] font-normal text-muted-foreground italic">
              (Stan Weinstein 4-Stage)
            </span>
          </h3>
          <p className="text-sm font-bold mt-0.5" style={{ color: meta.color }}>
            Stage {data.stage} — {meta.label}
          </p>
        </div>
      </div>

      {/* Mark felsefe yorumu */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Detay metrikler */}
      <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            30W MA
          </span>
          <span className="font-mono font-semibold tabular-nums">
            {fmtPrice(data.ma_value)}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            MA Sapma
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.price_vs_ma_pct == null
                  ? "var(--muted-foreground)"
                  : data.price_vs_ma_pct > 0
                  ? "var(--mtp-excellent)"
                  : "var(--mtp-danger)",
            }}
          >
            {fmtPct(data.price_vs_ma_pct, 1)}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Yıllık Eğim
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.slope_pct_per_year == null
                  ? "var(--muted-foreground)"
                  : data.slope_pct_per_year > 1
                  ? "var(--mtp-excellent)"
                  : data.slope_pct_per_year < -1
                  ? "var(--mtp-danger)"
                  : "var(--mtp-neutral)",
            }}
          >
            {fmtPct(data.slope_pct_per_year, 1)}
          </span>
        </div>
      </div>
    </div>
  );
}
