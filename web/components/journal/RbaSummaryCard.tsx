"use client";

import { useState } from "react";
import { TrendingUp, AlertTriangle, Info, CheckCircle2 } from "lucide-react";
import { useRbaMetrics, type RbaSeverity } from "@/hooks/use-rba-metrics";

/**
 * KARAR ADAY #722 — Mark RBA Summary Card
 *
 * Journal sayfasında üst banner: kullanıcının gerçek trade istatistikleri.
 * Mark TTLC Sec 4: "Know the truth about your trading."
 *
 * Min 30 trade gözlem → istatistiksel anlamlı (Mark kuralı).
 * Adjusted Ratio < 1.0 → CRITICAL (setup'ı bırakma uyarısı).
 */

const SEVERITY_COLORS: Record<RbaSeverity, { bg: string; text: string; border: string }> = {
  OK: {
    bg: "rgba(40,167,69,0.08)",
    text: "var(--mtp-excellent)",
    border: "var(--mtp-excellent)",
  },
  INFO: {
    bg: "rgba(75,156,211,0.08)",
    text: "var(--mtp-good, #4B9CD3)",
    border: "var(--mtp-good, #4B9CD3)",
  },
  WARNING: {
    bg: "rgba(245,158,11,0.10)",
    text: "#F59E0B",
    border: "#F59E0B",
  },
  CRITICAL: {
    bg: "rgba(220,53,69,0.10)",
    text: "var(--mtp-danger)",
    border: "var(--mtp-danger)",
  },
};

const SEVERITY_ICONS: Record<RbaSeverity, React.ReactNode> = {
  OK: <CheckCircle2 size={16} />,
  INFO: <Info size={16} />,
  WARNING: <AlertTriangle size={16} />,
  CRITICAL: <AlertTriangle size={16} />,
};

interface Props {
  strategy?: string;
  setupType?: string;
  /** Compact = inline tek satır; full = detay kart */
  variant?: "compact" | "full";
}

function fmtPct(value: number, decimals: number = 2): string {
  return `${value > 0 ? "+" : ""}${value.toFixed(decimals)}%`;
}

function fmtRatio(value: number): string {
  // Backend tum-kazanan edge'inde adjusted_ratio'yu 99.0 ile cap'ler (float('inf')
  // JSON serialize edilemez — Paket 348). 99+ = pratik sonsuz, "∞" goster.
  if (!isFinite(value) || value >= 99) return "∞";
  return value.toFixed(2);
}

export function RbaSummaryCard({ strategy, setupType, variant = "full" }: Props) {
  const { data, isLoading, isError } = useRbaMetrics({ strategy, setup_type: setupType });
  const [expanded, setExpanded] = useState(false);

  if (isLoading) {
    return (
      <div className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        Mark RBA hesaplanıyor...
      </div>
    );
  }

  if (isError || !data) {
    return null; // Sessiz fallback — RBA yoksa journal yine açılır
  }

  const m = data.metrics;
  const rec = data.recommendation;
  const colors = SEVERITY_COLORS[rec.severity];
  const winRatePct = m.win_rate * 100;

  if (variant === "compact") {
    return (
      <div
        className="rounded-md border px-3 py-2 flex items-center gap-3 text-xs"
        style={{ background: colors.bg, borderColor: `${colors.border}55` }}
      >
        <TrendingUp size={14} style={{ color: colors.text }} />
        <span className="font-semibold">RBA:</span>
        <span className="tabular-nums">{m.num_trades} trade</span>
        <span className="tabular-nums">Win {winRatePct.toFixed(0)}%</span>
        <span className="tabular-nums">Adj.R {fmtRatio(m.adjusted_ratio)}</span>
        <span className="tabular-nums">Exp {fmtPct(m.expectancy_pct, 1)}</span>
      </div>
    );
  }

  return (
    <div
      className="rounded-lg border p-4 flex flex-col gap-3"
      style={{ background: colors.bg, borderColor: `${colors.border}55` }}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2">
          <TrendingUp size={18} style={{ color: colors.text }} />
          <div>
            <h2 className="text-sm font-semibold flex items-center gap-2">
              Mark RBA
              <span className="text-xs font-normal text-muted-foreground italic">
                Result-Based Analysis (TTLC Sec 4)
              </span>
            </h2>
            {(strategy || setupType) && (
              <p className="text-xs text-muted-foreground mt-0.5">
                Filtre: {strategy && <span>strategy={strategy}</span>}
                {strategy && setupType && " · "}
                {setupType && <span>setup={setupType}</span>}
              </p>
            )}
          </div>
        </div>
        <span
          className="text-xs px-2 py-0.5 rounded font-medium inline-flex items-center gap-1"
          style={{
            background: colors.bg,
            color: colors.text,
            border: `1px solid ${colors.border}55`,
          }}
        >
          {SEVERITY_ICONS[rec.severity]}
          <span>{rec.severity}</span>
        </span>
      </div>

      {/* Severity message */}
      <p className="text-sm leading-relaxed" style={{ color: colors.text }}>
        {rec.message}
      </p>

      {/* Metric grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
        <div className="flex flex-col">
          <span className="text-muted-foreground">Trade Sayısı</span>
          <span className="text-base font-bold tabular-nums">
            {m.num_trades}
            {m.is_statistically_significant ? (
              <span className="text-xs font-normal ml-1" style={{ color: "var(--mtp-excellent)" }}>
                ✓ anlamlı
              </span>
            ) : (
              <span className="text-xs font-normal ml-1 text-muted-foreground">
                (min 30)
              </span>
            )}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground">Win Rate</span>
          <span className="text-base font-bold tabular-nums">
            {winRatePct.toFixed(1)}%
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground">Adjusted Ratio</span>
          <span
            className="text-base font-bold tabular-nums"
            style={{
              color:
                m.adjusted_ratio >= 1.5 ? "var(--mtp-excellent)" :
                m.adjusted_ratio >= 1.0 ? "var(--mtp-good, #4B9CD3)" :
                "var(--mtp-danger)",
            }}
          >
            {fmtRatio(m.adjusted_ratio)}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground">Expectancy</span>
          <span
            className="text-base font-bold tabular-nums"
            style={{
              color: m.expectancy_pct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
            }}
          >
            {fmtPct(m.expectancy_pct, 1)}
          </span>
        </div>
      </div>

      {/* Detay açılır kapalı */}
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="text-xs text-muted-foreground hover:text-foreground transition-colors text-left underline-offset-2 hover:underline"
      >
        {expanded ? "Detayı kapat" : "Detayı göster (avg gain/loss + largest)"}
      </button>

      {expanded && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs pt-2 border-t">
          <div className="flex flex-col">
            <span className="text-muted-foreground">Avg Gain</span>
            <span
              className="text-sm font-semibold tabular-nums"
              style={{ color: "var(--mtp-excellent)" }}
            >
              {fmtPct(m.avg_gain_pct, 2)}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-muted-foreground">Avg Loss</span>
            <span
              className="text-sm font-semibold tabular-nums"
              style={{ color: "var(--mtp-danger)" }}
            >
              {fmtPct(m.avg_loss_pct, 2)}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-muted-foreground">Largest Gain</span>
            <span
              className="text-sm font-semibold tabular-nums"
              style={{ color: "var(--mtp-excellent)" }}
            >
              {fmtPct(m.largest_gain_pct, 2)}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-muted-foreground">Largest Loss</span>
            <span
              className="text-sm font-semibold tabular-nums"
              style={{ color: "var(--mtp-danger)" }}
            >
              {fmtPct(m.largest_loss_pct, 2)}
            </span>
          </div>
        </div>
      )}

      {/* Mark birebir altyazı */}
      <p className="text-xs text-muted-foreground italic pt-1 border-t">
        Mark TTLC Sec 4 birebir: &ldquo;Know the truth about your trading.&rdquo;
        — Adjusted Ratio &lt; 1.0 setup&apos;ı bırakmaya işaret eder (KALICI İLKE #4).
      </p>
    </div>
  );
}
