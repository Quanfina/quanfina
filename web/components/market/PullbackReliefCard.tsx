"use client";

import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { usePullbackReliefRatio } from "@/hooks/use-pullback-relief-ratio";

/**
 * Paket 570 (20 Haz 2026): Pullback/Relief Rally oranı kartı (piyasa-durumu) — Carr s.280.
 *
 * Günlük Pullback aday / Relief Rally aday → piyasa rejim + aşırılık. >1 yükseliyor → boğa
 * oversold (AL dip); <1 düşüyor → ayı overbought (SHORT tepki). Kanon: Carr 2.baskı Böl.15
 * s.280. Veri yoksa "veri yok" (Kural #28). Relief=0 → aşırı boğa (payda 0).
 */
const REGIME_META: Record<string, { label: string; color: string; bg: string }> = {
  BULLISH_OVERSOLD: { label: "Boğa / Aşırı Satım — AL dip", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.10)" },
  BEARISH_OVERBOUGHT: { label: "Ayı / Aşırı Alım — SHORT tepki", color: "var(--mtp-danger)", bg: "rgba(220,53,69,0.10)" },
  NEUTRAL: { label: "Nötr / Geçiş", color: "#92400E", bg: "rgba(245,158,11,0.10)" },
};

export function PullbackReliefCard() {
  const { data, isLoading, isError } = usePullbackReliefRatio();

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Pullback/Relief oranı yükleniyor...
      </div>
    );
  }
  if (isError || !data || !data.available) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground" data-testid="pullback-relief-card">
        Pullback/Relief oranı: veri yok (tarama pvh erişilemez).
      </div>
    );
  }

  const meta = REGIME_META[data.regime ?? "NEUTRAL"] ?? REGIME_META.NEUTRAL;
  const DirIcon = data.direction === "rising" ? TrendingUp : data.direction === "falling" ? TrendingDown : Minus;
  const ratioText = data.ratio != null ? data.ratio.toFixed(2) : "—";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
      data-testid="pullback-relief-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color: meta.color }}><DirIcon size={16} /></span>
        Pullback/Relief Oranı (Carr s.280)
      </h3>

      <div className="flex items-baseline gap-2 pt-1 border-t border-muted-foreground/15">
        <span className="font-mono font-bold text-lg tabular-nums" style={{ color: meta.color }}>
          {ratioText}
        </span>
        <span className="text-[11px] font-semibold uppercase tracking-wide" style={{ color: meta.color }}>
          {meta.label}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-[11px]">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Pullback (LONG)</span>
          <span className="font-mono font-semibold tabular-nums" style={{ color: "var(--mtp-excellent)" }}>
            {data.pullback_count}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Relief (SHORT)</span>
          <span className="font-mono font-semibold tabular-nums" style={{ color: "var(--mtp-danger)" }}>
            {data.relief_count}
          </span>
        </div>
      </div>

      <p className="text-[10px] text-muted-foreground leading-relaxed">{data.mark_says}</p>
    </div>
  );
}
