"use client";

import { Calculator, TrendingUp, CheckCircle2, AlertCircle, XCircle } from "lucide-react";
import {
  useTradesExpectancy,
  type BrandonExpectancyCategory,
} from "@/hooks/use-trades-expectancy";

/**
 * KARAR #733 alt-paket (Paket 90, 26 May 2026): Brandon Expectancy /journal widget.
 * Mark Brandon Video canon UI tezahürü:
 *   E = (P_win × avg_gain) - (P_loss × avg_loss)
 *   Hedef: %20 avg gain / %4 avg loss / %50 win rate / E >= %10.
 *
 * 4 kategori:
 * - EXCELLENT (E >= 15%): 🟢 Brandon canon zirvesi
 * - GOOD (E >= 10%): 🟢 Brandon eşiği
 * - ACCEPTABLE (E >= 5%): 🟡 Asgari kârlılık
 * - POOR (E < 5%): 🔴 Disiplin gerek
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<BrandonExpectancyCategory, CategoryMeta> = {
  EXCELLENT: {
    label: "Brandon Canon Zirvesi",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.12)",
    icon: <TrendingUp size={16} />,
  },
  GOOD: {
    label: "Brandon Eşiği OK",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.08)",
    icon: <CheckCircle2 size={16} />,
  },
  ACCEPTABLE: {
    label: "Asgari Kârlılık",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertCircle size={16} />,
  },
  POOR: {
    label: "Disiplin Gerek",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.10)",
    icon: <XCircle size={16} />,
  },
};

export function BrandonExpectancyCard({ strategy }: { strategy?: string } = {}) {
  const { data, isLoading, isError } = useTradesExpectancy(strategy);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Brandon Expectancy yükleniyor...
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-xs text-destructive">
        Brandon Expectancy alınamadı.
      </div>
    );
  }

  // Hiç kapalı trade yoksa minimal panel
  if (!data.category || data.total_closed === 0) {
    return (
      <div className="rounded-lg border p-3 flex flex-col gap-1.5 text-xs">
        <div className="flex items-center gap-2 font-semibold">
          <Calculator size={16} />
          Brandon Expectancy
        </div>
        <p className="text-muted-foreground italic">{data.mark_says}</p>
      </div>
    );
  }

  const meta = CATEGORY_META[data.category];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Calculator size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Brandon Expectancy
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark Brandon Video)
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

      {/* Expectancy büyük rakam */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          E =
        </span>
        <span
          className="text-2xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.expectancy_pct > 0 ? "+" : ""}
          {data.expectancy_pct.toFixed(2)}%
        </span>
        <span className="text-[10px] text-muted-foreground italic ml-auto">
          /trade
        </span>
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1 text-foreground/80"
        style={{ color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Metrik grid: 4 alan */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <Metric
          label="Avg Gain"
          value={`+${data.avg_gain_pct.toFixed(1)}%`}
          target=">=20%"
          met={data.avg_gain_pct >= 20}
        />
        <Metric
          label="Avg Loss"
          value={`-${data.avg_loss_pct.toFixed(1)}%`}
          target="<=4%"
          met={data.avg_loss_pct <= 4 && data.avg_loss_pct > 0}
        />
        <Metric
          label="Win Rate"
          value={`${(data.win_rate * 100).toFixed(0)}%`}
          target=">=50%"
          met={data.win_rate >= 0.5}
        />
        <Metric
          label="Kapalı"
          value={`${data.winners}W/${data.losers}L`}
          target={`${data.total_closed} toplam`}
          met={null}
        />
      </div>
    </div>
  );
}

function Metric({
  label,
  value,
  target,
  met,
}: {
  label: string;
  value: string;
  target: string;
  met: boolean | null;
}) {
  const color =
    met === null
      ? "var(--foreground)"
      : met
      ? "var(--mtp-excellent)"
      : "var(--mtp-danger)";
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
        {label}
      </span>
      <span
        className="font-mono font-semibold tabular-nums"
        style={{ color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
      >
        {value}
      </span>
      <span className="text-[9px] text-muted-foreground italic">{target}</span>
    </div>
  );
}
