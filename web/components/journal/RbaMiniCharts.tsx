"use client";

import { useMemo } from "react";
import { useTrades } from "@/hooks/use-trades";
import { computeMonthlyRba } from "@/lib/monthly-rba";

/**
 * Paket 401: 4 mini RBA chart galeri (inline SVG, dep-free).
 *
 * Markets360 "DRMA Charts" pattern uyarlaması (clean-room — kendi isim/yapı).
 * Bizimkiler:
 *   1. Win Rate Trend (line, aylık)
 *   2. Avg Gain vs Avg Loss (paralel bar, aylık)
 *   3. Aylık Net % (bar, pozitif yeşil / negatif kırmızı)
 *   4. Equity Curve (cumulative P&L line)
 *
 * Mark TTLC Sec 4 RBA — görsel performans takibi. İLKE #11 (Objektif Ayna Dil):
 * grafikler sayısal, motivasyon dili yok.
 *
 * SVG inline (Recharts/Chart.js dep yok — bundle minimum).
 */

const CHART_W = 200;
const CHART_H = 80;
const PAD = { top: 8, right: 4, bottom: 16, left: 24 };

function MiniBar({
  title,
  values,
  labels,
  unit,
  colorPositive,
  colorNegative,
  signed = true,
}: {
  title: string;
  values: number[];
  labels: string[];
  unit: string;
  colorPositive: string;
  colorNegative: string;
  signed?: boolean;
}) {
  if (values.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-3">
        <h4 className="text-xs font-medium mb-1">{title}</h4>
        <p className="text-[10px] text-muted-foreground">Veri yok</p>
      </div>
    );
  }
  const max = Math.max(...values.map((v) => Math.abs(v)), 0.01);
  const innerH = CHART_H - PAD.top - PAD.bottom;
  const innerW = CHART_W - PAD.left - PAD.right;
  const barW = Math.max(4, innerW / values.length - 2);
  const zeroY = signed ? PAD.top + innerH / 2 : PAD.top + innerH;
  return (
    <div className="rounded-lg border bg-card p-3">
      <h4 className="text-xs font-medium mb-1 flex items-center justify-between">
        <span>{title}</span>
        <span className="text-[10px] text-muted-foreground">{values.length} ay</span>
      </h4>
      <svg viewBox={`0 0 ${CHART_W} ${CHART_H}`} className="w-full">
        {/* Zero line */}
        {signed && (
          <line
            x1={PAD.left} x2={CHART_W - PAD.right}
            y1={zeroY} y2={zeroY}
            stroke="currentColor" strokeOpacity={0.2} strokeDasharray="2 2"
          />
        )}
        {values.map((v, i) => {
          const h = signed
            ? (Math.abs(v) / max) * (innerH / 2)
            : (Math.abs(v) / max) * innerH;
          const x = PAD.left + i * (innerW / values.length) + 1;
          const y = v >= 0 ? zeroY - h : zeroY;
          const color = v >= 0 ? colorPositive : colorNegative;
          return (
            <g key={i}>
              <rect
                x={x} y={y}
                width={barW} height={Math.max(1, h)}
                fill={color} opacity={0.85}
              >
                <title>{`${labels[i]}: ${v.toFixed(1)}${unit}`}</title>
              </rect>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function MiniLine({
  title,
  values,
  labels,
  unit,
  color,
  isCumulative = false,
}: {
  title: string;
  values: number[];
  labels: string[];
  unit: string;
  color: string;
  isCumulative?: boolean;
}) {
  if (values.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-3">
        <h4 className="text-xs font-medium mb-1">{title}</h4>
        <p className="text-[10px] text-muted-foreground">Veri yok</p>
      </div>
    );
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 0.01);
  const innerH = CHART_H - PAD.top - PAD.bottom;
  const innerW = CHART_W - PAD.left - PAD.right;
  const step = values.length > 1 ? innerW / (values.length - 1) : innerW;
  const points = values
    .map((v, i) => {
      const x = PAD.left + i * step;
      const y = PAD.top + (1 - (v - min) / range) * innerH;
      return `${x},${y}`;
    })
    .join(" ");
  // Cumulative için 0 satırı görünür
  const zeroY =
    isCumulative && min < 0 && max > 0
      ? PAD.top + (1 - (0 - min) / range) * innerH
      : null;
  return (
    <div className="rounded-lg border bg-card p-3">
      <h4 className="text-xs font-medium mb-1 flex items-center justify-between">
        <span>{title}</span>
        <span className="text-[10px] text-muted-foreground">{values.length} ay</span>
      </h4>
      <svg viewBox={`0 0 ${CHART_W} ${CHART_H}`} className="w-full">
        {zeroY != null && (
          <line
            x1={PAD.left} x2={CHART_W - PAD.right}
            y1={zeroY} y2={zeroY}
            stroke="currentColor" strokeOpacity={0.2} strokeDasharray="2 2"
          />
        )}
        <polyline
          points={points}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
        />
        {values.map((v, i) => {
          const x = PAD.left + i * step;
          const y = PAD.top + (1 - (v - min) / range) * innerH;
          return (
            <circle key={i} cx={x} cy={y} r="2" fill={color}>
              <title>{`${labels[i]}: ${v.toFixed(1)}${unit}`}</title>
            </circle>
          );
        })}
      </svg>
    </div>
  );
}

export function RbaMiniCharts() {
  const trades = useTrades();
  const monthlyRows = useMemo(
    () => computeMonthlyRba(trades.data ?? []),
    [trades.data],
  );

  // En eski → en yeni sırada (chronological, soldan sağa)
  const chronological = useMemo(() => [...monthlyRows].reverse(), [monthlyRows]);
  const labels = chronological.map((r) => r.monthLabel);
  const winRates = chronological.map((r) => r.winRate);
  const avgGains = chronological.map((r) => r.avgGainPct);
  const avgLosses = chronological.map((r) => r.avgLossPct);
  const netPcts = chronological.map((r) => r.netPct);

  // Equity curve: kümülatif P&L dolar
  const equityCurve = useMemo(() => {
    let cum = 0;
    return chronological.map((r) => {
      cum += r.totalPlDollar;
      return cum;
    });
  }, [chronological]);

  return (
    <div className="grid grid-cols-2 gap-3" data-testid="rba-mini-charts">
      <MiniLine
        title="Win Rate Trend"
        values={winRates}
        labels={labels}
        unit="%"
        color="#4B9CD3"
      />
      <MiniBar
        title="Avg Gain — Aylık"
        values={avgGains}
        labels={labels}
        unit="%"
        colorPositive="#28A745"
        colorNegative="#28A745"
        signed={false}
      />
      <MiniBar
        title="Avg Loss — Aylık"
        values={avgLosses}
        labels={labels}
        unit="%"
        colorPositive="#FF5733"
        colorNegative="#FF5733"
        signed={false}
      />
      <MiniLine
        title="Equity Curve ($)"
        values={equityCurve}
        labels={labels}
        unit="$"
        color="#F59E0B"
        isCumulative
      />
    </div>
  );
}
