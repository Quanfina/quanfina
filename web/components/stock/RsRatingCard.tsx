"use client";

import { Award, Crown, TrendingUp, Minus, ArrowDown } from "lucide-react";
import { useRsRating, type RsRatingCategory } from "@/hooks/use-rs-rating";

/**
 * KARAR #733 alt-paket (Paket 94, 26 May 2026): RS Rating /hisse detay paneli.
 * Mark TLSMW Ch 3-5 / IBD canon Relative Strength Rating (1-99) UI tezahürü.
 *
 * 4 kategori:
 * - LEADER (>=80): 👑 IBD canon "Top 20%", alım adayı
 * - STRONG (70-79): ↗ üst orta dilim, izleme
 * - AVERAGE (50-69): ▪ vasat
 * - LAGGARD (<50): ↓ alt dilim, Mark "uzak dur"
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<RsRatingCategory, CategoryMeta> = {
  LEADER: {
    label: "IBD LEADER",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.12)",
    icon: <Crown size={16} />,
  },
  STRONG: {
    label: "STRONG",
    color: "var(--mtp-neutral)",
    bg: "rgba(75,156,211,0.10)",
    icon: <TrendingUp size={16} />,
  },
  AVERAGE: {
    label: "AVERAGE",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.08)",
    icon: <Minus size={16} />,
  },
  LAGGARD: {
    label: "LAGGARD",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.10)",
    icon: <ArrowDown size={16} />,
  },
};

export function RsRatingCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useRsRating(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        RS Rating yükleniyor...
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
        <Award size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          RS Rating
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 3-5 / IBD)
          </span>
          {/* P441: taranmamış sembol → yerel yaklaşık (cross-sectional değil) */}
          {data.source === "computed" && (
            <span
              className="ml-1.5 text-[9px] font-semibold px-1 py-0.5 rounded uppercase"
              style={{ background: "rgba(245,158,11,0.15)", color: "#F59E0B" }}
              title="Taranmamış sembol — yerel yaklaşık RS (cross-sectional persentil değil). Tam IBD RS için günlük taramada olmalı."
            >
              ~yaklaşık
            </span>
          )}
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: meta.color, color: "#fff" }}
        >
          {meta.icon}
          {meta.label}
        </span>
      </div>

      {/* Büyük RS rakam */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          RS
        </span>
        <span
          className="text-2xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.rs_rating ?? "—"}
        </span>
        <span className="text-[10px] text-muted-foreground italic ml-auto">
          / 99
        </span>
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1"
        style={{ color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Metrik 3 kolon */}
      <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Hisse
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.stock_return_pct == null
                  ? "var(--muted-foreground)"
                  : data.stock_return_pct > 0
                  ? "var(--mtp-excellent)"
                  : "var(--mtp-danger)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
            title="12 ay ağırlıklı getiri (Q1 %40 + Q2/Q3/Q4 %20)"
          >
            {data.stock_return_pct != null
              ? `${data.stock_return_pct > 0 ? "+" : ""}${data.stock_return_pct.toFixed(1)}%`
              : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            SPY
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {data.benchmark_return_pct != null
              ? `${data.benchmark_return_pct > 0 ? "+" : ""}${data.benchmark_return_pct.toFixed(1)}%`
              : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Outperform
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.outperform_pct == null
                  ? "var(--muted-foreground)"
                  : data.outperform_pct >= 20
                  ? "var(--mtp-excellent)"
                  : data.outperform_pct > 0
                  ? "var(--mtp-neutral)"
                  : "var(--mtp-danger)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
            title="Hisse - SPY weighted return farkı"
          >
            {data.outperform_pct != null
              ? `${data.outperform_pct > 0 ? "+" : ""}${data.outperform_pct.toFixed(1)}%`
              : "—"}
          </span>
        </div>
      </div>
    </div>
  );
}
