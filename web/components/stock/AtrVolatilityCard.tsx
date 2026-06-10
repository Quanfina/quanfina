"use client";

import { Activity, Wind, Minus, AlertTriangle, AlertOctagon } from "lucide-react";
import { useAtrVolatility, type AtrCategory } from "@/hooks/use-atr-volatility";
import { fmtUsd } from "@/lib/format-currency";

/**
 * KARAR #733 alt-paket (Paket 103, 26 May 2026): ATR Volatility /hisse detay.
 * Mark TLSMW Ch 11 / Wilder canon ATR UI tezahürü — stop placement disiplini.
 *
 * 4 kategori:
 * - LOW (<2%): Mark VCP final daralma adayı
 * - NORMAL (2-4%): Sağlıklı trend
 * - HIGH (4-7%): Stop genişletilmeli + pozisyon küçültülmeli
 * - EXTREME (>7%): Mark "fight the noise" - kaçın
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<AtrCategory, CategoryMeta> = {
  LOW: {
    label: "Sıkı",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <Wind size={16} />,
  },
  NORMAL: {
    label: "Sağlıklı",
    color: "var(--mtp-neutral)",
    bg: "rgba(75,156,211,0.10)",
    icon: <Minus size={16} />,
  },
  HIGH: {
    label: "Yüksek",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertTriangle size={16} />,
  },
  EXTREME: {
    label: "Aşırı",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.12)",
    icon: <AlertOctagon size={16} />,
  },
};

export function AtrVolatilityCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useAtrVolatility(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        ATR yükleniyor...
      </div>
    );
  }

  if (isError || !data || !data.category) return null;

  const meta = CATEGORY_META[data.category];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Activity size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          ATR Volatilite
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 11 / Wilder)
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

      {/* ATR rakam */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          ATR
        </span>
        <span
          className="text-xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.atr != null ? fmtUsd(data.atr) : "—"}
        </span>
        {data.atr_pct != null && (
          <span
            className="text-sm font-mono tabular-nums opacity-80"
            style={{ color: meta.color }}
          >
            ({data.atr_pct.toFixed(1)}%)
          </span>
        )}
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1"
        style={{ color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* 3 stop seviyesi */}
      <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Sıkı 2×
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ color: "var(--mtp-danger)", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            title="Pivot bazlı sıkı stop (Mark: stop hunters aktif)"
          >
            {data.suggested_stop_tight != null ? fmtUsd(data.suggested_stop_tight) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Normal 2.5×
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ color: "#F59E0B", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            title="Standart Mark canon stop"
          >
            {data.suggested_stop_normal != null ? fmtUsd(data.suggested_stop_normal) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Gevşek 3×
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ color: "var(--mtp-excellent)", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            title="Trail için gevşek stop (room to breathe)"
          >
            {data.suggested_stop_loose != null ? fmtUsd(data.suggested_stop_loose) : "—"}
          </span>
        </div>
      </div>
    </div>
  );
}
