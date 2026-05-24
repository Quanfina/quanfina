"use client";

import { CheckCircle2, AlertCircle, XCircle, Shield, Crown, Activity, TrendingUp, TrendingDown, AlertTriangle, Sunrise } from "lucide-react";
import { computeMarketRegime, type MarketRegimeInfo } from "@/lib/market-regime";
import type {
  MarkRegimeInfoBackend,
  MarketBreadthInfo,
  BreadthDivergenceInfo,
  FollowThroughDayInfo,
} from "@/types/market";

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
  // KARAR ADAY #735 alt-paket (Paket 27): SPY/QQQ/IWM Stage özet (Mark+Carr)
  spyStage?: number;
  qqqStage?: number;
  iwmStage?: number;
  // KARAR #733 alt-paket (Paket 53): Market Breadth A/D Line (P51+P52 backend)
  marketBreadth?: MarketBreadthInfo | null;
  // KARAR #733 alt-paket (Paket 58): Index vs A/D Divergence (P56+P57 backend)
  breadthDivergence?: BreadthDivergenceInfo | null;
  // KARAR #733 alt-paket (Paket 66): Mark/O'Neil Follow-Through Day (P64+P65 backend)
  followThrough?: FollowThroughDayInfo | null;
}

// KARAR #733 alt-paket (Paket 58): Divergence kategori meta — 5 kategori
const DIVERGENCE_META: Record<
  "CONFIRMED_UP" | "BEARISH_DIVERGENCE" | "BULLISH_DIVERGENCE" | "CONFIRMED_DOWN" | "NEUTRAL",
  { color: string; bg: string; icon: React.ReactNode; label: string; shortLabel: string }
> = {
  CONFIRMED_UP: {
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <CheckCircle2 size={14} />,
    label: "Onaylı Rally",
    shortLabel: "ONAYLI ↑",
  },
  BEARISH_DIVERGENCE: {
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.12)",
    icon: <AlertTriangle size={14} />,
    label: "Bearish Divergence (KRİTİK)",
    shortLabel: "BEARISH ⚠️",
  },
  BULLISH_DIVERGENCE: {
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75,156,211,0.10)",
    icon: <TrendingUp size={14} />,
    label: "Bullish Divergence",
    shortLabel: "BULLISH ↗",
  },
  CONFIRMED_DOWN: {
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.08)",
    icon: <TrendingDown size={14} />,
    label: "Onaylı Düşüş",
    shortLabel: "ONAYLI ↓",
  },
  NEUTRAL: {
    color: "var(--muted-foreground)",
    bg: "rgba(127,127,127,0.06)",
    icon: <Activity size={14} />,
    label: "Belirsiz",
    shortLabel: "NEUTRAL",
  },
};

const BREADTH_META: Record<"STRONG" | "NEUTRAL" | "WEAK", { color: string; icon: React.ReactNode; label: string }> = {
  STRONG: {
    color: "var(--mtp-excellent)",
    icon: <TrendingUp size={12} />,
    label: "Saglikli",
  },
  NEUTRAL: {
    color: "#F59E0B",
    icon: <Activity size={12} />,
    label: "Karisik",
  },
  WEAK: {
    color: "var(--mtp-danger)",
    icon: <TrendingDown size={12} />,
    label: "Zayif",
  },
};

const STAGE_META: Record<number, { emoji: string; label: string; color: string }> = {
  1: { emoji: "⏳", label: "Basing", color: "var(--mtp-good, #4B9CD3)" },
  2: { emoji: "📈", label: "Advancing", color: "var(--mtp-excellent)" },
  3: { emoji: "⚠️", label: "Topping", color: "#F59E0B" },
  4: { emoji: "⛔", label: "Declining", color: "var(--mtp-danger)" },
};

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

export function MarkRegimeCard({
  distributionDays,
  backendRegime,
  spyStage,
  qqqStage,
  iwmStage,
  marketBreadth,
  breadthDivergence,
  followThrough,
}: Props) {
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

      {/* KARAR #735 alt (Paket 27): SPY/QQQ/IWM Stage özet (Mark+Carr) */}
      {(spyStage || qqqStage || iwmStage) && (
        <div className="flex items-center gap-2 text-xs pt-2 border-t border-muted-foreground/15">
          <span className="text-muted-foreground">Endeks Stage:</span>
          <div className="flex items-center gap-3 ml-auto">
            {spyStage && STAGE_META[spyStage] && (
              <span
                className="inline-flex items-center gap-1 tabular-nums"
                title={`SPY Carr Stage ${spyStage} - ${STAGE_META[spyStage].label}`}
                style={{ color: STAGE_META[spyStage].color }}
              >
                <span aria-hidden="true">{STAGE_META[spyStage].emoji}</span>
                <span className="font-semibold">SPY {spyStage}</span>
              </span>
            )}
            {qqqStage && STAGE_META[qqqStage] && (
              <span
                className="inline-flex items-center gap-1 tabular-nums"
                title={`QQQ Carr Stage ${qqqStage} - ${STAGE_META[qqqStage].label}`}
                style={{ color: STAGE_META[qqqStage].color }}
              >
                <span aria-hidden="true">{STAGE_META[qqqStage].emoji}</span>
                <span className="font-semibold">QQQ {qqqStage}</span>
              </span>
            )}
            {iwmStage && STAGE_META[iwmStage] && (
              <span
                className="inline-flex items-center gap-1 tabular-nums"
                title={`IWM Carr Stage ${iwmStage} - ${STAGE_META[iwmStage].label}`}
                style={{ color: STAGE_META[iwmStage].color }}
              >
                <span aria-hidden="true">{STAGE_META[iwmStage].emoji}</span>
                <span className="font-semibold">IWM {iwmStage}</span>
              </span>
            )}
          </div>
        </div>
      )}

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

      {/* KARAR #733 alt-paket (Paket 53): Market Breadth A/D Line widget */}
      {marketBreadth && (
        <div className="pt-2 border-t border-muted-foreground/15 flex flex-col gap-1.5">
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground flex items-center gap-1.5">
              <Activity size={12} />
              Market Breadth (A/D)
            </span>
            <span
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
              style={{
                background: `${BREADTH_META[marketBreadth.breadth_health].color}22`,
                color: BREADTH_META[marketBreadth.breadth_health].color,
                border: `1px solid ${BREADTH_META[marketBreadth.breadth_health].color}55`,
              }}
              title={marketBreadth.mark_says}
            >
              {BREADTH_META[marketBreadth.breadth_health].icon}
              {BREADTH_META[marketBreadth.breadth_health].label}
            </span>
          </div>
          <div className="flex items-center gap-4 text-xs tabular-nums">
            <span className="text-muted-foreground">
              A/D Ratio:{" "}
              <span
                className="font-mono font-semibold"
                style={{ color: BREADTH_META[marketBreadth.breadth_health].color }}
              >
                {marketBreadth.ad_ratio.toFixed(2)}
              </span>
            </span>
            <span className="text-muted-foreground ml-auto">
              20g Birikim:{" "}
              <span
                className="font-mono font-semibold"
                style={{
                  color:
                    marketBreadth.ad_line_cumulative > 0
                      ? "var(--mtp-excellent)"
                      : "var(--mtp-danger)",
                }}
              >
                {marketBreadth.ad_line_cumulative > 0 ? "+" : ""}
                {marketBreadth.ad_line_cumulative.toLocaleString("en-US")}
              </span>
            </span>
          </div>
          <p className="text-[10px] text-muted-foreground italic leading-snug">
            {marketBreadth.mark_says}
          </p>
        </div>
      )}

      {/* KARAR #733 alt-paket (Paket 58): Index vs A/D Divergence widget */}
      {breadthDivergence && (
        <div
          className="pt-2 border-t border-muted-foreground/15 flex flex-col gap-1.5 rounded-md"
          style={{
            // KRİTİK durum (BEARISH_DIVERGENCE) için ekstra vurgu
            background:
              breadthDivergence.divergence === "BEARISH_DIVERGENCE"
                ? DIVERGENCE_META.BEARISH_DIVERGENCE.bg
                : undefined,
            padding:
              breadthDivergence.divergence === "BEARISH_DIVERGENCE"
                ? "0.5rem"
                : undefined,
            border:
              breadthDivergence.divergence === "BEARISH_DIVERGENCE"
                ? `1px solid ${DIVERGENCE_META.BEARISH_DIVERGENCE.color}55`
                : undefined,
          }}
        >
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground flex items-center gap-1.5">
              {DIVERGENCE_META[breadthDivergence.divergence].icon}
              Index × A/D Divergence
            </span>
            <span
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
              style={{
                background: `${DIVERGENCE_META[breadthDivergence.divergence].color}22`,
                color: DIVERGENCE_META[breadthDivergence.divergence].color,
                border: `1px solid ${DIVERGENCE_META[breadthDivergence.divergence].color}55`,
              }}
              title={DIVERGENCE_META[breadthDivergence.divergence].label}
            >
              {DIVERGENCE_META[breadthDivergence.divergence].shortLabel}
            </span>
          </div>
          <div className="flex items-center gap-4 text-xs tabular-nums">
            <span className="text-muted-foreground">
              SPY {breadthDivergence.index_change_pct >= 0 ? "+" : ""}
              <span
                className="font-mono font-semibold"
                style={{
                  color:
                    breadthDivergence.index_change_pct > 0
                      ? "var(--mtp-excellent)"
                      : breadthDivergence.index_change_pct < 0
                      ? "var(--mtp-danger)"
                      : "var(--muted-foreground)",
                }}
              >
                {breadthDivergence.index_change_pct.toFixed(2)}%
              </span>
            </span>
            <span className="text-muted-foreground ml-auto">
              A/D Trend:{" "}
              <span
                className="font-mono font-semibold"
                style={{
                  color:
                    breadthDivergence.ad_trend_delta > 0
                      ? "var(--mtp-excellent)"
                      : breadthDivergence.ad_trend_delta < 0
                      ? "var(--mtp-danger)"
                      : "var(--muted-foreground)",
                }}
              >
                {breadthDivergence.ad_trend_delta > 0 ? "+" : ""}
                {breadthDivergence.ad_trend_delta.toLocaleString("en-US")}
              </span>
            </span>
          </div>
          <p
            className="text-[10px] italic leading-snug"
            style={{
              color:
                breadthDivergence.severity === "critical"
                  ? "var(--mtp-danger)"
                  : "var(--muted-foreground)",
              fontWeight:
                breadthDivergence.severity === "critical" ? 600 : "normal",
            }}
          >
            {breadthDivergence.mark_says}
          </p>
        </div>
      )}

      {/* KARAR #733 alt-paket (Paket 66): Follow-Through Day widget —
          FTD ONAYLI durum yeşil bant (Mark Stage 2 başlangıç sinyali),
          FTD ZAYIF (hacim eksik) sarı uyarı. ftd_detected=False ise gizli. */}
      {followThrough && followThrough.ftd_detected && (
        <div
          className="pt-2 border-t border-muted-foreground/15 flex flex-col gap-1.5 rounded-md"
          style={{
            background: followThrough.volume_confirmed
              ? "rgba(40,167,69,0.10)"
              : "rgba(245,158,11,0.10)",
            padding: "0.5rem",
            border: followThrough.volume_confirmed
              ? "1px solid rgba(40,167,69,0.40)"
              : "1px solid rgba(245,158,11,0.40)",
          }}
        >
          <div className="flex items-center justify-between text-xs">
            <span
              className="flex items-center gap-1.5 font-semibold"
              style={{
                color: followThrough.volume_confirmed
                  ? "var(--mtp-excellent)"
                  : "#F59E0B",
              }}
            >
              <Sunrise size={14} />
              Follow-Through Day
            </span>
            <span
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
              style={{
                background: followThrough.volume_confirmed
                  ? "var(--mtp-excellent)"
                  : "#F59E0B",
                color: "#fff",
              }}
              title={followThrough.mark_says}
            >
              {followThrough.volume_confirmed ? "✓ ONAYLI" : "⚠️ ZAYIF"}
            </span>
          </div>
          <div className="flex items-center gap-4 text-xs tabular-nums">
            {followThrough.ftd_gain_pct != null && (
              <span className="text-muted-foreground">
                FTD Day:{" "}
                <span
                  className="font-mono font-semibold"
                  style={{
                    color: followThrough.volume_confirmed
                      ? "var(--mtp-excellent)"
                      : "#F59E0B",
                  }}
                >
                  +{followThrough.ftd_gain_pct.toFixed(2)}%
                </span>
              </span>
            )}
            {followThrough.days_after_low != null && (
              <span className="text-muted-foreground">
                Dip&apos;ten:{" "}
                <span className="font-mono font-semibold tabular-nums">
                  {followThrough.days_after_low} gün
                </span>
              </span>
            )}
            {followThrough.previous_low != null && (
              <span className="text-muted-foreground ml-auto">
                Dip:{" "}
                <span className="font-mono font-semibold">
                  ${followThrough.previous_low.toFixed(2)}
                </span>
              </span>
            )}
          </div>
          <p
            className="text-[10px] italic leading-snug"
            style={{
              color: followThrough.volume_confirmed
                ? "var(--mtp-excellent)"
                : "#F59E0B",
            }}
          >
            {followThrough.mark_says}
          </p>
        </div>
      )}

      {/* Mark felsefesi atıf */}
      <p className="text-[11px] text-muted-foreground italic pt-1 border-t border-muted-foreground/15">
        KARAR #488 (Vizyon v20.99) — Mark 4-Katman × O&apos;Neil mekanik.
        DD 4+ = O&apos;Neil Hard Filter, Eksen 2 Lider Override felsefesi.
        {marketBreadth && (
          <> KARAR #733 (Paket 51-53) — Mark+O&apos;Neil A/D Line breadth.</>
        )}
        {breadthDivergence && (
          <> KARAR #733 (Paket 56-58) — Index×A/D Divergence (1999/2007 tarihsel paten).</>
        )}
        {followThrough?.ftd_detected && (
          <> KARAR #733 (Paket 64-66) — Mark/O&apos;Neil FTD (2003/2009/2020 dip recovery).</>
        )}
      </p>
    </div>
  );
}
