"use client";

import { useMemo } from "react";
import { useTrades } from "@/hooks/use-trades";
import { useMarketStatus } from "@/hooks/use-market-status";
import { useTradingMode } from "@/hooks/use-trading-mode";
import { usePortfolioValue } from "@/hooks/use-portfolio-value";
import { MARKET_HEALTH_LABEL_TR, type MarketHealthLabel } from "@/types/market";

/**
 * Paket 404: Tüm sayfalarda görünür kompakt sticky özet bar.
 *
 * Markets360 üst Portfolio (DTS/RTS/P&L) sayılar pattern'inin Quanfina
 * uyarlaması (clean-room — kendi terim/yapı). Sn. Ferit hangi sayfada
 * olursa olsun en kritik 4 bilgiyi görür:
 *   - Trading Mode (Normal / Rehab / Defansif / Agresif)
 *   - Market Health (Sağlıklı / Nötr / Baskı Altında)
 *   - Açık trade sayısı + toplam dolar risk (Mark "Risk first")
 *   - Realized P&L (kapalı trade'ler net)
 *
 * Mark "Risk first" disiplini görsel uyum — açık pozisyon riski ön planda.
 * İLKE #11 Objektif Ayna Dil — sayı + birim, motivasyon yok.
 *
 * Veri yoksa (DB down / hook loading) sessizce minimal render — UX kesintisiz.
 */

const MODE_COLORS: Record<string, string> = {
  normal: "var(--muted-foreground)",
  agresif: "var(--mtp-excellent)",
  rehab: "#F59E0B",
  defansif: "var(--mtp-danger)",
};

const HEALTH_COLORS: Record<MarketHealthLabel, string> = {
  HEALTHY: "var(--mtp-excellent)",
  NEUTRAL: "var(--mtp-neutral)",
  UNDER_PRESSURE: "var(--mtp-danger)",
};

export function QuickSummaryBar() {
  const trades = useTrades();
  const market = useMarketStatus();
  const tradingMode = useTradingMode();
  const { value: portfolioValue } = usePortfolioValue();

  const stats = useMemo(() => {
    const all = trades.data ?? [];
    const open = all.filter((t) => t.status === "open");
    const closed = all.filter((t) => t.status === "closed");
    const realizedPlDollar = closed.reduce((s, t) => s + (t.pl_dollar ?? 0), 0);

    // Toplam dolar risk: sum((entry - plan_stop) × shares) açık trade'lerde
    let totalDollarRisk = 0;
    for (const t of open) {
      if (t.plan_stop != null && t.plan_stop > 0) {
        totalDollarRisk += (t.entry_price - t.plan_stop) * t.shares;
      }
    }
    const totalPctRisk = portfolioValue > 0 ? (totalDollarRisk / portfolioValue) * 100 : 0;

    return {
      openCount: open.length,
      totalDollarRisk,
      totalPctRisk,
      realizedPlDollar,
      hasData: all.length > 0,
    };
  }, [trades.data, portfolioValue]);

  // Hook'lar yükleniyorsa veya hiç data yoksa minimal render (yer kaplamasın)
  if (!stats.hasData && !market.data && !tradingMode) return null;

  const modeLabel = tradingMode.mode.charAt(0).toUpperCase() + tradingMode.mode.slice(1);
  const modeColor = MODE_COLORS[tradingMode.mode] ?? "var(--muted-foreground)";

  const healthLabel = market.data?.market_health_label;
  const healthColor = healthLabel ? HEALTH_COLORS[healthLabel] : "var(--muted-foreground)";
  const healthTr = healthLabel ? MARKET_HEALTH_LABEL_TR[healthLabel] : "—";

  const riskColor =
    stats.totalPctRisk > 2.5
      ? "var(--mtp-danger)"
      : stats.totalPctRisk > 1.5
      ? "#F59E0B"
      : "var(--mtp-excellent)";

  const plColor =
    stats.realizedPlDollar > 0
      ? "var(--mtp-excellent)"
      : stats.realizedPlDollar < 0
      ? "var(--mtp-danger)"
      : "var(--muted-foreground)";

  return (
    <div
      role="status"
      aria-label="Quanfina özet — Trading Mode, Market Health, Açık Risk, Realized P&L"
      data-testid="quick-summary-bar"
      className="sticky top-0 z-30 flex flex-wrap items-center gap-x-4 gap-y-1 px-4 py-1.5 border-b bg-background/80 backdrop-blur-sm text-xs"
    >
      {/* Trading Mode */}
      <div className="flex items-center gap-1.5">
        <span className="text-muted-foreground">Mod:</span>
        <span className="font-semibold" style={{ color: modeColor }}>
          {modeLabel}
        </span>
        <span className="text-muted-foreground tabular-nums">
          %{tradingMode.recommendedSizingPct.toFixed(1)}
        </span>
      </div>

      <span className="text-muted-foreground/40 hidden sm:inline">·</span>

      {/* Market Health */}
      <div className="flex items-center gap-1.5">
        <span className="text-muted-foreground">Piyasa:</span>
        <span className="font-semibold" style={{ color: healthColor }}>
          {healthTr}
        </span>
      </div>

      <span className="text-muted-foreground/40 hidden sm:inline">·</span>

      {/* Açık trade + risk (Mark "Risk first") */}
      <div className="flex items-center gap-1.5">
        <span className="text-muted-foreground">Açık:</span>
        <span className="font-semibold tabular-nums">{stats.openCount}</span>
        {stats.totalDollarRisk > 0 && (
          <span
            className="tabular-nums"
            style={{ color: riskColor }}
            title="Toplam dolar risk (entry - plan_stop) × shares — Mark Risk first"
          >
            -${Math.abs(stats.totalDollarRisk).toFixed(0)} (-%{stats.totalPctRisk.toFixed(1)})
          </span>
        )}
      </div>

      <span className="text-muted-foreground/40 hidden sm:inline">·</span>

      {/* Realized P&L */}
      <div className="flex items-center gap-1.5">
        <span className="text-muted-foreground">Realized:</span>
        <span className="font-semibold tabular-nums" style={{ color: plColor }}>
          {stats.realizedPlDollar >= 0 ? "+" : ""}${stats.realizedPlDollar.toFixed(0)}
        </span>
      </div>
    </div>
  );
}
