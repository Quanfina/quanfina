"use client";

import { useEffect, useRef } from "react";
import { useTheme } from "next-themes";
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
} from "lightweight-charts";
import type { OhlcvBar } from "@/types/stock";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const CHART_HEIGHT = 380;
const VOLUME_HEIGHT = 90;

function computeMA(data: OhlcvBar[], period: number): (number | null)[] {
  return data.map((_, i) => {
    if (i < period - 1) return null;
    const sum = data.slice(i - period + 1, i + 1).reduce((s, b) => s + b.close, 0);
    return sum / period;
  });
}

interface PriceChartProps {
  data: OhlcvBar[];
}

export function PriceChart({ data }: PriceChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === "dark";

  useEffect(() => {
    if (!containerRef.current || !data.length) return;

    const bg          = isDark ? "#0f0f0f" : "#ffffff";
    const textColor   = isDark ? "#9ca3af" : "#374151";
    const gridColor   = isDark ? "#1f2937" : "#f3f4f6";
    const borderColor = isDark ? "#374151" : "#e5e7eb";

    const chart = createChart(containerRef.current, {
      layout: { background: { color: bg }, textColor },
      grid: {
        vertLines: { color: gridColor },
        horzLines: { color: gridColor },
      },
      rightPriceScale: { borderColor },
      timeScale: { borderColor, timeVisible: true, secondsVisible: false },
      width: containerRef.current.clientWidth,
      height: CHART_HEIGHT + VOLUME_HEIGHT,
    });

    // Candlestick — pane 0
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    }, 0);
    candleSeries.setData(
      data.map((b) => ({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close }))
    );

    // MA50 — pane 0 (amber)
    const ma50vals = computeMA(data, 50);
    const ma50Series = chart.addSeries(LineSeries, {
      color: "#f59e0b",
      lineWidth: 1,
      title: "MA50",
      lastValueVisible: true,
      priceLineVisible: false,
    }, 0);
    ma50Series.setData(
      data
        .map((b, i) => ({ time: b.time, value: ma50vals[i] }))
        .filter((d): d is { time: string; value: number } => d.value != null)
    );

    // MA200 — pane 0 (violet)
    const ma200vals = computeMA(data, 200);
    const ma200Series = chart.addSeries(LineSeries, {
      color: "#8b5cf6",
      lineWidth: 1,
      title: "MA200",
      lastValueVisible: true,
      priceLineVisible: false,
    }, 0);
    ma200Series.setData(
      data
        .map((b, i) => ({ time: b.time, value: ma200vals[i] }))
        .filter((d): d is { time: string; value: number } => d.value != null)
    );

    // Volume histogram — pane 1
    const volSeries = chart.addSeries(HistogramSeries, {
      color: isDark ? "rgba(99,102,241,0.4)" : "rgba(99,102,241,0.3)",
      priceFormat: { type: "volume" },
      lastValueVisible: false,
      priceLineVisible: false,
    }, 1);
    volSeries.setData(
      data.map((b) => ({
        time: b.time,
        value: b.volume,
        color: b.close >= b.open
          ? (isDark ? "rgba(34,197,94,0.45)"  : "rgba(34,197,94,0.5)")
          : (isDark ? "rgba(239,68,68,0.45)" : "rgba(239,68,68,0.5)"),
      }))
    );

    // Volume pane height
    const panes = chart.panes();
    if (panes[1]) panes[1].setHeight(VOLUME_HEIGHT);

    chart.timeScale().fitContent();

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, [data, isDark]);

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-0.5 bg-amber-400" />
          <TermTooltip termKey="ma50">MA50</TermTooltip>
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-0.5 bg-violet-500" />
          <TermTooltip termKey="ma200">MA200</TermTooltip>
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-2 h-2 rounded-sm bg-green-500 opacity-70" />
          <TermTooltip termKey="candlestick">Candlestick</TermTooltip>
        </span>
      </div>
      <div ref={containerRef} style={{ width: "100%" }} />
    </div>
  );
}
