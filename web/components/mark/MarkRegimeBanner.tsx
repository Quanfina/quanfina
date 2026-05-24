"use client";

/**
 * KARAR #733 alt-paket (Paket 36): Mark Regime üst-uyarı banner.
 *
 * Sinyaller + Watchlist + (gelecek) Hisse Tarama sayfalarinin üstüne yerleşir.
 * Kullanici hisse seçmeye başlamadan ÖNCE bugünkü piyasa rejimini görür —
 * UNDER_PRESSURE / BEAR_PRESSURE iken kırmızı banner + "yeni alım YASAK"
 * felsefe uyarısı.
 *
 * Mark felsefe: "Don't fight the tape." Bear bazaarda yeni alım = disiplinsizlik.
 *
 * DRY: backend /api/market/status mark_regime alanı (Paket 35 doğrulandı).
 * Hisse listesi varsa Stage 4 sayısı eklenir (extra mekanik karar metrik).
 */

import { AlertTriangle, ShieldAlert, ShieldCheck, Activity } from "lucide-react";
import { useMarketStatus } from "@/hooks/use-market-status";
import type { MarkRegimeType } from "@/types/market";

interface RegimeMeta {
  emoji: string;
  color: string;
  bg: string;
  borderColor: string;
  icon: React.ReactNode;
  severity: "info" | "neutral" | "warning" | "critical";
}

const REGIME_META: Record<MarkRegimeType, RegimeMeta> = {
  HEALTHY: {
    emoji: "🟢",
    color: "var(--mtp-excellent)",
    bg: "rgba(40, 167, 69, 0.08)",
    borderColor: "rgba(40, 167, 69, 0.35)",
    icon: <ShieldCheck size={18} />,
    severity: "info",
  },
  CAUTION: {
    emoji: "🟡",
    color: "#F59E0B",
    bg: "rgba(245, 158, 11, 0.08)",
    borderColor: "rgba(245, 158, 11, 0.40)",
    icon: <Activity size={18} />,
    severity: "neutral",
  },
  UNDER_PRESSURE: {
    emoji: "🟠",
    color: "#EA580C",
    bg: "rgba(234, 88, 12, 0.10)",
    borderColor: "rgba(234, 88, 12, 0.45)",
    icon: <AlertTriangle size={18} />,
    severity: "warning",
  },
  BEAR_PRESSURE: {
    emoji: "🔴",
    color: "var(--mtp-danger)",
    bg: "rgba(220, 53, 69, 0.10)",
    borderColor: "rgba(220, 53, 69, 0.50)",
    icon: <ShieldAlert size={18} />,
    severity: "critical",
  },
};

interface MarkRegimeBannerProps {
  /** Listedeki Carr Stage = 4 olan hisse sayısı (varsa ek uyarı). */
  stage4Count?: number;
  /** Toplam list size (oran göstermek için). */
  totalCount?: number;
  /** Compact mode = sadece tek satır (cleaner sayfa üstü). */
  compact?: boolean;
  /** Kullanıcının dismiss etmesi için (gelecek). */
  hideOnHealthy?: boolean;
}

export function MarkRegimeBanner({
  stage4Count = 0,
  totalCount = 0,
  compact = false,
  hideOnHealthy = true,
}: MarkRegimeBannerProps) {
  const { data, isLoading, isError } = useMarketStatus();

  if (isLoading || isError || !data?.mark_regime) return null;

  const regime = data.mark_regime;
  const meta = REGIME_META[regime.regime];

  // KARAR #733 alt-paket (Paket 61): Kritik Divergence yakalama
  // BEARISH_DIVERGENCE (severity=critical) durumu — 1999/2007 paten erken uyarı
  const divergence = data.breadth_divergence;
  const isCriticalDivergence = divergence?.severity === "critical";

  // HEALTHY iken sessiz olabilir (gürültü azaltma) — Sn. Ferit tercih flag
  // İSTİSNA (Paket 61): Kritik divergence varsa HEALTHY iken bile gösterilir
  if (
    hideOnHealthy &&
    regime.regime === "HEALTHY" &&
    stage4Count === 0 &&
    !isCriticalDivergence
  ) {
    return null;
  }

  const dd = data.distribution_days;
  const stage4Pct = totalCount > 0 ? Math.round((stage4Count / totalCount) * 100) : 0;
  const showStage4Warn = stage4Count > 0;

  if (compact) {
    return (
      <div
        className="flex items-center gap-2 px-3 py-1.5 border-b text-xs"
        style={{ background: meta.bg, borderColor: meta.borderColor, color: meta.color }}
        role="status"
      >
        <span aria-hidden="true">{meta.emoji}</span>
        <span className="font-semibold">{regime.label}</span>
        <span className="opacity-80">·</span>
        <span>{regime.allocation}</span>
        {isCriticalDivergence && (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
            style={{ background: "var(--mtp-danger)", color: "#fff" }}
            title={divergence!.mark_says}
          >
            BEARISH ⚠️
          </span>
        )}
        <span className="opacity-60 ml-auto">DD {dd}/20</span>
      </div>
    );
  }

  return (
    <div
      className="px-4 py-3 border-b flex items-start gap-3"
      style={{ background: meta.bg, borderColor: meta.borderColor }}
      role={meta.severity === "critical" ? "alert" : "status"}
    >
      <span aria-hidden="true" className="text-2xl leading-none mt-0.5">
        {meta.emoji}
      </span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span style={{ color: meta.color }}>{meta.icon}</span>
          <h3 className="text-sm font-bold" style={{ color: meta.color }}>
            Piyasa Rejimi: {regime.label}
          </h3>
          {!regime.new_buy_allowed && (
            <span
              className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
              style={{ background: meta.color, color: "#fff" }}
            >
              Yeni Alım YASAK
            </span>
          )}
        </div>
        <p className="text-xs leading-relaxed" style={{ color: meta.color }}>
          <span className="font-semibold">Allocation:</span> {regime.allocation}
          {regime.pilot_override && regime.new_buy_allowed === false && (
            <span className="opacity-80">
              {" "}· İstisna: Lider hisse %1-2 pilot pozisyon Override
            </span>
          )}
        </p>
        <p className="text-[11px] mt-1 opacity-75" style={{ color: meta.color }}>
          Distribution Days son 20 günde: <span className="font-mono font-semibold">{dd}/20</span>
          {showStage4Warn && (
            <>
              {" "}· Bu listede{" "}
              <span className="font-semibold">
                {stage4Count} hisse Carr Stage 4 ({stage4Pct}%)
              </span>{" "}
              — UZAK DUR adayları
            </>
          )}
        </p>

        {/* KARAR #733 alt-paket (Paket 61): Kritik divergence uyarısı —
            BEARISH_DIVERGENCE (severity=critical) durumunda tüm sayfalarda
            5 sayfa banner DRY uyarı. 1999 dot-com / 2007 housing tarihsel
            paten Mark+O'Neil erken uyari canon. */}
        {isCriticalDivergence && divergence && (
          <div
            className="mt-2 px-2 py-1.5 rounded text-[11px] flex items-start gap-2"
            style={{
              background: "rgba(220,53,69,0.18)",
              border: "1px solid var(--mtp-danger)",
              color: "var(--mtp-danger)",
            }}
          >
            <AlertTriangle size={14} className="shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-1.5 mb-0.5">
                <span className="font-bold uppercase tracking-wider">
                  BEARISH DIVERGENCE
                </span>
                <span className="opacity-75 font-mono tabular-nums">
                  · SPY {divergence.index_change_pct >= 0 ? "+" : ""}
                  {divergence.index_change_pct.toFixed(2)}% / A/D{" "}
                  {divergence.ad_trend_delta > 0 ? "+" : ""}
                  {divergence.ad_trend_delta.toLocaleString("en-US")}
                </span>
              </div>
              <p className="opacity-90 leading-snug">{divergence.mark_says}</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
