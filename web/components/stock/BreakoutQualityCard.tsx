"use client";

import { Award, CheckCircle2, ThumbsUp, AlertTriangle, XCircle } from "lucide-react";
import { useBreakoutQuality, type BreakoutQualityCategory } from "@/hooks/use-breakout-quality";

/**
 * KARAR #733 alt-paket (Paket 134, 26 May 2026): BreakoutQualityCard /hisse detay.
 * Mark TLSMW Ch 10 pivot kırılım kalite kompoziti 0-100 puan UI tezahürü.
 *
 * 4 kategori:
 * - EXCELLENT (>=85): A++ kırılım, full size
 * - GOOD (70-84): Standart canon, normal size
 * - MARGINAL (50-69): Pilot pozisyon
 * - POOR (<50): Mark "alma" — fake breakout riski
 *
 * Breakdown (5 kriter):
 * - Hacim teyit (30 puan)
 * - Gap-up (20 puan)
 * - Breakout strength %1.7+ (25 puan)
 * - Prior contraction (15 puan)
 * - Overhead clean (10 puan)
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<BreakoutQualityCategory, CategoryMeta> = {
  EXCELLENT: {
    label: "A++ Kırılım",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.12)",
    icon: <CheckCircle2 size={16} />,
  },
  GOOD: {
    label: "Standart",
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75,156,211,0.10)",
    icon: <ThumbsUp size={16} />,
  },
  MARGINAL: {
    label: "Zayıf Teyit",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertTriangle size={16} />,
  },
  POOR: {
    label: "Fake Riski",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.10)",
    icon: <XCircle size={16} />,
  },
};

const CRITERIA_LABELS: Record<string, { label: string; max: number }> = {
  volume: { label: "Hacim", max: 30 },
  gap_up: { label: "Gap-Up", max: 20 },
  breakout_strength: { label: "Güç", max: 25 },
  prior_contraction: { label: "VCP", max: 15 },
  overhead_clean: { label: "Temiz", max: 10 },
};

export function BreakoutQualityCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useBreakoutQuality(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Kırılım kalitesi yükleniyor...
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
          Kırılım Kalitesi
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

      {/* Büyük skor */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          Skor
        </span>
        <span
          className="text-2xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.score}
        </span>
        <span className="text-[10px] text-muted-foreground italic">/ 100</span>
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1"
        style={{ color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Breakdown 5 kriter */}
      <div className="grid grid-cols-5 gap-1 text-xs pt-1 border-t border-muted-foreground/15">
        {(Object.keys(CRITERIA_LABELS) as Array<keyof typeof CRITERIA_LABELS>).map(
          (k) => {
            const value = data.breakdown[k as keyof typeof data.breakdown] ?? 0;
            const max = CRITERIA_LABELS[k].max;
            const passed = value === max;
            const partial = value > 0 && value < max;
            return (
              <div
                key={k}
                className="flex flex-col items-center gap-0.5 rounded px-1 py-1"
                style={{
                  background: passed
                    ? "rgba(40,167,69,0.10)"
                    : partial
                    ? "rgba(245,158,11,0.10)"
                    : "rgba(220,53,69,0.06)",
                }}
                title={`${CRITERIA_LABELS[k].label} — ${value}/${max} puan`}
              >
                <span className="text-[9px] text-muted-foreground uppercase tracking-wider">
                  {CRITERIA_LABELS[k].label}
                </span>
                <span
                  className="font-mono font-bold tabular-nums text-[11px]"
                  style={{
                    color: passed
                      ? "var(--mtp-excellent)"
                      : partial
                      ? "#F59E0B"
                      : "var(--mtp-danger)",
                  }}
                >
                  {value}
                </span>
              </div>
            );
          }
        )}
      </div>
    </div>
  );
}
