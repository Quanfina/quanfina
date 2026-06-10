"use client";

import { useEffect, useRef, useState } from "react";
import { useTheme } from "next-themes";
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  LineStyle,
  type IChartApi,
} from "lightweight-charts";
import type { OhlcvBar } from "@/types/stock";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const CHART_HEIGHT = 380;
const VOLUME_HEIGHT = 90;

// P438-M3: zaman aralığı seçenekleri (bar = işlem günü ~). null = tümü.
const RANGES: { label: string; bars: number | null }[] = [
  { label: "3A", bars: 63 },
  { label: "6A", bars: 126 },
  { label: "1Y", bars: 252 },
  { label: "Tüm", bars: null },
];

function computeMA(data: OhlcvBar[], period: number): (number | null)[] {
  return data.map((_, i) => {
    if (i < period - 1) return null;
    const sum = data.slice(i - period + 1, i + 1).reduce((s, b) => s + b.close, 0);
    return sum / period;
  });
}

interface PriceChartProps {
  data: OhlcvBar[];
  /** P438-M3: karar çizgileri — Mark canon referans (canvas hex renk). */
  pivotPrice?: number | null;
  stopPrice?: number | null;
}

interface Legend {
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  time: string;
  up: boolean;
}

export function PriceChart({ data, pivotPrice, stopPrice }: PriceChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartApiRef = useRef<IChartApi | null>(null);
  // P438-M3: seçili aralık rebuild'de (tema değişimi) korunsun
  const selectedBarsRef = useRef<number | null>(252);
  const [activeRange, setActiveRange] = useState<string>("1Y");
  const [legend, setLegend] = useState<Legend | null>(null);
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === "dark";

  // P438-M3: seçili aralığı uygula (chart hazır + buton tıklaması)
  function applyRange(bars: number | null) {
    const chart = chartApiRef.current;
    if (!chart || !data.length) return;
    if (bars == null || data.length <= bars) {
      chart.timeScale().fitContent();
    } else {
      chart.timeScale().setVisibleLogicalRange({ from: data.length - bars, to: data.length - 1 });
    }
  }

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
    chartApiRef.current = chart;

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

    // P438-M3: karar çizgileri — Mark canon referans noktaları (canvas hex).
    // Pivot (mavi kesik) = alım noktası, Stop (kırmızı kesik) = ATR 2.5×,
    // 52W yüksek/düşük (gri noktalı) = aralık bağlamı.
    if (pivotPrice != null && pivotPrice > 0) {
      candleSeries.createPriceLine({
        price: pivotPrice,
        color: "#4b9cd3",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: "Pivot",
      });
    }
    if (stopPrice != null && stopPrice > 0) {
      candleSeries.createPriceLine({
        price: stopPrice,
        color: "#ef4444",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: "Stop",
      });
    }
    // 52W high/low — son ~252 bar (yoksa tümü)
    const w52 = data.slice(-252);
    if (w52.length > 0) {
      const high52 = Math.max(...w52.map((b) => b.high));
      const low52 = Math.min(...w52.map((b) => b.low));
      candleSeries.createPriceLine({
        price: high52, color: "#6b7280", lineWidth: 1, lineStyle: LineStyle.Dotted,
        axisLabelVisible: true, title: "52H Y",
      });
      candleSeries.createPriceLine({
        price: low52, color: "#6b7280", lineWidth: 1, lineStyle: LineStyle.Dotted,
        axisLabelVisible: true, title: "52H D",
      });
    }

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

    // P438-M3: crosshair OHLC okuma — hover'da bar değerleri legend'e.
    const lastBar = data[data.length - 1];
    setLegend({
      o: lastBar.open, h: lastBar.high, l: lastBar.low, c: lastBar.close,
      v: lastBar.volume, time: lastBar.time, up: lastBar.close >= lastBar.open,
    });
    chart.subscribeCrosshairMove((param) => {
      const cd = param.seriesData.get(candleSeries) as
        | { open: number; high: number; low: number; close: number }
        | undefined;
      const vd = param.seriesData.get(volSeries) as { value: number } | undefined;
      if (!cd || param.time == null) {
        // crosshair grafik dışı → son bar
        setLegend({
          o: lastBar.open, h: lastBar.high, l: lastBar.low, c: lastBar.close,
          v: lastBar.volume, time: lastBar.time, up: lastBar.close >= lastBar.open,
        });
        return;
      }
      setLegend({
        o: cd.open, h: cd.high, l: cd.low, c: cd.close,
        v: vd?.value ?? 0, time: String(param.time), up: cd.close >= cd.open,
      });
    });

    // P435/P438-M3: seçili aralığı uygula (default 1Y, rebuild'de korunur)
    applyRange(selectedBarsRef.current);

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartApiRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, isDark, pivotPrice, stopPrice]);

  function fmtVol(v: number): string {
    if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return String(v);
  }

  return (
    <div className="flex flex-col gap-2">
      {/* Üst bar: MA/candle açıklama + zaman aralığı seçici */}
      <div className="flex items-center justify-between gap-2 flex-wrap">
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
        {/* Zaman aralığı seçici */}
        <div className="flex items-center gap-0.5 rounded-md border p-0.5">
          {RANGES.map((r) => (
            <button
              key={r.label}
              type="button"
              onClick={() => {
                selectedBarsRef.current = r.bars;
                setActiveRange(r.label);
                applyRange(r.bars);
              }}
              className={`text-[11px] px-2 py-0.5 rounded transition-colors ${
                activeRange === r.label
                  ? "bg-accent text-foreground font-semibold"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* P438-M3: crosshair OHLC legend — Mark "fiyat aksiyonu oku" */}
      {legend && (
        <div className="flex items-center gap-2.5 text-[11px] font-mono tabular-nums text-muted-foreground" style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}>
          <span className="text-foreground/60">{legend.time}</span>
          <span>A <span className="text-foreground">{legend.o.toFixed(2)}</span></span>
          <span>Y <span className="text-foreground">{legend.h.toFixed(2)}</span></span>
          <span>D <span className="text-foreground">{legend.l.toFixed(2)}</span></span>
          <span>
            K{" "}
            <span style={{ color: legend.up ? "#22c55e" : "#ef4444" }} className="font-semibold">
              {legend.c.toFixed(2)}
            </span>
          </span>
          <span>Hcm <span className="text-foreground">{fmtVol(legend.v)}</span></span>
        </div>
      )}

      <div ref={containerRef} style={{ width: "100%" }} />

      {/* Karar çizgileri açıklama */}
      <div className="flex items-center gap-3 text-[10px] text-muted-foreground flex-wrap">
        {pivotPrice != null && pivotPrice > 0 && (
          <span className="flex items-center gap-1">
            <span className="inline-block w-3 border-t border-dashed" style={{ borderColor: "#4b9cd3" }} />
            Pivot (alım noktası)
          </span>
        )}
        {stopPrice != null && stopPrice > 0 && (
          <span className="flex items-center gap-1">
            <span className="inline-block w-3 border-t border-dashed" style={{ borderColor: "#ef4444" }} />
            Stop (ATR 2.5×)
          </span>
        )}
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 border-t border-dotted" style={{ borderColor: "#6b7280" }} />
          52H yüksek / düşük
        </span>
      </div>
    </div>
  );
}
