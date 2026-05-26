"use client";

import { Layers, Zap, CheckCircle2, Hourglass, Minus } from "lucide-react";
import { useStageTransition, type StageCategory } from "@/hooks/use-stage-transition";
import { fmtUsd } from "@/lib/format-currency";

/**
 * KARAR #733 alt-paket (Paket 122, 26 May 2026): Stage Transition /hisse detay.
 * Mark TLSMW Ch 4 + Weinstein Stage 1→2 geçiş UI tezahürü.
 *
 * 4 kategori:
 * - NO_TRANSITION: ⚪ Stage 1/3/4, kırılım yok
 * - EARLY_STAGE_2: ⚡ Yeni kırılım (fragile)
 * - CONFIRMED_STAGE_2: ✓ Trend kanıtlı
 * - STAGE_2_MATURE: ⏳ Olgun trend (climax riski)
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<StageCategory, CategoryMeta> = {
  NO_TRANSITION: {
    label: "Kırılım Yok",
    color: "var(--muted-foreground)",
    bg: "rgba(128,128,128,0.06)",
    icon: <Minus size={16} />,
  },
  EARLY_STAGE_2: {
    label: "Erken Stage 2",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <Zap size={16} />,
  },
  CONFIRMED_STAGE_2: {
    label: "Stage 2 Onaylı",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <CheckCircle2 size={16} />,
  },
  STAGE_2_MATURE: {
    label: "Olgun Trend",
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75,156,211,0.10)",
    icon: <Hourglass size={16} />,
  },
};

const VOLUME_META: Record<string, { label: string; color: string }> = {
  RISING: { label: "↑ Artıyor", color: "var(--mtp-excellent)" },
  STABLE: { label: "→ Düz", color: "var(--muted-foreground)" },
  FALLING: { label: "↓ Düşüyor", color: "var(--mtp-danger)" },
};

export function StageTransitionCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useStageTransition(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Stage Transition yükleniyor...
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
        <Layers size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Stage Transition
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 4 / Weinstein)
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
        className="text-xs italic leading-relaxed px-2 py-1 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* 4 metrik grid */}
      <div className="grid grid-cols-4 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            30W MA
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            title="30 haftalık (150 günlük) hareketli ortalama"
          >
            {data.ma_value != null ? fmtUsd(data.ma_value) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            MA Üstü
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.price_above_ma_pct == null
                  ? "var(--muted-foreground)"
                  : data.price_above_ma_pct > 0
                  ? "var(--mtp-excellent)"
                  : "var(--mtp-danger)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
          >
            {data.price_above_ma_pct != null
              ? `${data.price_above_ma_pct > 0 ? "+" : ""}${data.price_above_ma_pct.toFixed(1)}%`
              : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Üstte Gün
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.days_above_ma == null
                  ? "var(--muted-foreground)"
                  : data.days_above_ma >= 60
                  ? "var(--mtp-good, #4B9CD3)"
                  : data.days_above_ma >= 10
                  ? "var(--mtp-excellent)"
                  : "#F59E0B",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
            title="30W MA üzerinde geçen gün (1-10 fragile, 10+ kanıtlı, 60+ olgun)"
          >
            {data.days_above_ma != null ? `${data.days_above_ma}g` : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Hacim
          </span>
          {data.volume_trend ? (
            <span
              className="font-semibold tabular-nums"
              style={{ color: VOLUME_META[data.volume_trend].color, fontSize: 11 }}
              title="Son 10 gün / önceki 20 gün ortalama hacim oranı"
            >
              {VOLUME_META[data.volume_trend].label}
            </span>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </div>
      </div>
    </div>
  );
}
