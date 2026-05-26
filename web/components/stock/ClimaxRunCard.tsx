"use client";

import { Flame, AlertOctagon, AlertTriangle, TrendingUp, Minus } from "lucide-react";
import { useClimaxRun, type ClimaxRunCategory } from "@/hooks/use-climax-run";

/**
 * KARAR #733 alt-paket (Paket 88, 26 May 2026): Climax Run detay paneli /hisse/[symbol].
 * Mark TLSMW Ch 9 "Selling Short and Climax Runs" canon UI tezahürü.
 *
 * 4 durum:
 * - CLIMAX_TOP: 🔴 SAT/Çıkış Sinyali (parabolic + gap exhaustion teyitli)
 * - POTENTIAL_CLIMAX: 🟡 Dikkat Uyarısı (büyük kazanç ama tam kanıt yok)
 * - HEALTHY_ADVANCE: 🟢 Normal Trend (let winners run)
 * - NONE: ⚪ Trend yok / negatif
 *
 * Mark felsefe: "When everyone is screaming buy at the top, that's when you sell."
 */

interface CategoryMeta {
  emoji: string;
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<ClimaxRunCategory, CategoryMeta> = {
  CLIMAX_TOP: {
    emoji: "🔴",
    label: "SAT/Çıkış",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.12)",
    icon: <AlertOctagon size={16} />,
  },
  POTENTIAL_CLIMAX: {
    emoji: "⚠️",
    label: "Dikkat",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertTriangle size={16} />,
  },
  HEALTHY_ADVANCE: {
    emoji: "✅",
    label: "Sağlıklı Trend",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <TrendingUp size={16} />,
  },
  NONE: {
    emoji: "⚪",
    label: "Trend Yok",
    color: "var(--muted-foreground)",
    bg: "rgba(128,128,128,0.06)",
    icon: <Minus size={16} />,
  },
};

export function ClimaxRunCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useClimaxRun(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Climax Run yükleniyor...
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
        <Flame size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Climax Run
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 9)
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

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Metrikler */}
      <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            15g Kazanç
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.gain_pct == null
                  ? "var(--muted-foreground)"
                  : data.gain_pct >= 25
                  ? "var(--mtp-danger)"
                  : data.gain_pct >= 15
                  ? "#F59E0B"
                  : data.gain_pct > 0
                  ? "var(--mtp-excellent)"
                  : "var(--muted-foreground)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
          >
            {data.gain_pct != null
              ? `${data.gain_pct > 0 ? "+" : ""}${data.gain_pct.toFixed(1)}%`
              : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Gap-Up
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.gap_up_days == null
                  ? "var(--muted-foreground)"
                  : data.gap_up_days >= 2
                  ? "var(--mtp-danger)"
                  : "var(--foreground)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
            title="Exhaustion gap sayısı (open >=%3 prev close)"
          >
            {data.gap_up_days != null ? `${data.gap_up_days} gün` : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Hacim Oranı
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.avg_volume_ratio == null
                  ? "var(--muted-foreground)"
                  : data.avg_volume_ratio >= 2
                  ? "var(--mtp-danger)"
                  : data.avg_volume_ratio >= 1.5
                  ? "#F59E0B"
                  : "var(--foreground)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
            title="Son 5g ortalama / önceki 10g ortalama (>=2 = exhaustion)"
          >
            {data.avg_volume_ratio != null
              ? `${data.avg_volume_ratio.toFixed(2)}x`
              : "—"}
          </span>
        </div>
      </div>
    </div>
  );
}
