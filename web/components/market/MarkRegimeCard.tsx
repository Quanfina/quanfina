"use client";

import { CheckCircle2, AlertCircle, XCircle, Shield, Crown } from "lucide-react";
import { computeMarketRegime, type MarketRegimeInfo } from "@/lib/market-regime";
import type { MarkRegimeInfoBackend } from "@/types/market";

/**
 * KARAR ADAY #729 (24 May 2026) — Mark Market Regime Kartı.
 *
 * KARAR #488 (Vizyon v20.99) — Market Regime 4-Katman × 2-Eksen Matris.
 * Mark birebir + O'Neil mekanik + Spot SPY %25 dilim allocation.
 *
 * 4 Katman: HEALTHY / CAUTION / UNDER_PRESSURE / BEAR_PRESSURE
 * Eksen 2 Override: Lider hisse %1-2 pilot delebilir
 *
 * KARAR #731 (24 May 2026): Backend mark_regime tercih + client-side fallback (DRY).
 */

interface Props {
  distributionDays: number;
  /** Backend pre-compute (KARAR #731) — varsa client-side hesap yerine kullanılır */
  backendRegime?: MarkRegimeInfoBackend | null;
  /** Spot SPY model placeholder — gelecek genişleme */
  spySpotAllocationPct?: number;
}

const REGIME_ICON = {
  HEALTHY: <CheckCircle2 size={20} />,
  CAUTION: <AlertCircle size={20} />,
  UNDER_PRESSURE: <Shield size={20} />,
  BEAR_PRESSURE: <XCircle size={20} />,
};

// Backend MarkRegimeInfoBackend -> client MarketRegimeInfo eşleştirme (DRY)
function backendToInfo(b: MarkRegimeInfoBackend, dd: number): MarketRegimeInfo {
  // Helper'in REGIME_MAP'inden görsel meta'lar (color, bgColor, emoji, markSays)
  // alınır; backend sadece felsefi alanları (allocation, new_buy_allowed,
  // pilot_override, label) override eder.
  const fallback = computeMarketRegime(dd);
  return {
    ...fallback,
    regime: b.regime,
    label: b.label,
    allocation: b.allocation,
    newBuyAllowed: b.new_buy_allowed,
    pilotOverride: b.pilot_override,
  };
}

export function MarkRegimeCard({ distributionDays, backendRegime }: Props) {
  // KARAR #731 — Backend tercih + client-side fallback (DRY)
  const info: MarketRegimeInfo = backendRegime
    ? backendToInfo(backendRegime, distributionDays)
    : computeMarketRegime(distributionDays);

  return (
    <div
      className="rounded-lg border p-4 flex flex-col gap-3"
      style={{
        background: info.bgColor,
        borderColor: `${info.color}55`,
      }}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2">
          <span aria-hidden="true" className="text-2xl leading-none">
            {info.emoji}
          </span>
          <div>
            <h2 className="text-sm font-semibold flex items-center gap-2">
              <span style={{ color: info.color }}>{REGIME_ICON[info.regime]}</span>
              Mark Market Regime
            </h2>
            <p
              className="text-lg font-bold mt-0.5"
              style={{ color: info.color }}
            >
              {info.label}
            </p>
          </div>
        </div>
        <span
          className="text-xs px-2 py-1 rounded font-mono font-semibold"
          style={{ background: `${info.color}22`, color: info.color }}
          title="Son 4-5 hafta Distribution Day sayısı (O'Neil mekanik)"
        >
          {distributionDays} DD
        </span>
      </div>

      {/* Mark birebir alıntı */}
      <p
        className="text-sm leading-relaxed italic px-3 py-2 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: info.color }}
      >
        {info.markSays}
      </p>

      {/* Allocation öneri */}
      <div className="flex items-center justify-between text-xs">
        <span className="text-muted-foreground">Mark Allocation:</span>
        <span className="font-semibold" style={{ color: info.color }}>
          {info.allocation}
        </span>
      </div>

      {/* Disiplin matrisi */}
      <div className="grid grid-cols-2 gap-2 text-xs pt-2 border-t border-muted-foreground/15">
        <div className="flex items-center gap-2 px-2 py-1.5 rounded bg-background/40">
          <span className="text-muted-foreground">Yeni Alım:</span>
          <span
            className="font-semibold"
            style={{
              color: info.newBuyAllowed ? "var(--mtp-excellent)" : "var(--mtp-danger)",
            }}
          >
            {info.newBuyAllowed ? "✓ İzinli" : "✗ YASAK"}
          </span>
        </div>
        <div className="flex items-center gap-2 px-2 py-1.5 rounded bg-background/40">
          <Crown size={12} className="text-muted-foreground" />
          <span className="text-muted-foreground">Lider Override:</span>
          <span
            className="font-semibold"
            style={{ color: info.pilotOverride ? "var(--mtp-good, #4B9CD3)" : "var(--muted-foreground)" }}
          >
            {info.pilotOverride ? "%1-2 pilot OK" : "—"}
          </span>
        </div>
      </div>

      {/* Mark felsefesi atıf */}
      <p className="text-[11px] text-muted-foreground italic pt-1 border-t border-muted-foreground/15">
        KARAR #488 (Vizyon v20.99) — Mark 4-Katman × O&apos;Neil mekanik.
        DD 4+ = O&apos;Neil Hard Filter, Eksen 2 Lider Override felsefesi.
      </p>
    </div>
  );
}
