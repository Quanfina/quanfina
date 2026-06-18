"use client";

import { TrendingUp, Minus } from "lucide-react";
import { usePullback } from "@/hooks/use-pullback";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 506 (18 Haz 2026): Carr Pullback paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "The Pullback" (s.249 entry + s.321-324 exit) — WEAK_BULL trend-takip LONG,
 * Carr'ın amiral pullback setup'ı (Mean Reversion countertrend'i tamamlar). Sinyal varsa
 * giriş(signal high üstü)/stop(50MA-%2 / %8 cap)/hedef(2R); yoksa SMA + Stoch %K bağlamı.
 * Çift danışma (Carr NotebookLM) ile kanon doğrulandı. Veri: SMA200 → ≥200 bar gerek.
 */
export function PullbackCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = usePullback(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Carr Pullback yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const color = data.detected ? "var(--mtp-excellent)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(40, 167, 69, 0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-pullback-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <TrendingUp size={16} /> : <Minus size={16} />}
        </span>
        Carr Pullback
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Trend-takip)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-pullback-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;200 bar veri — paper trade için
          güvenilmez (SMA200 gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🟢 LONG sinyal (Pullback)" : "Sinyal yok"}
      </p>

      {data.detected ? (
        <>
          <p
            className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
            style={{ borderLeftColor: color, color }}
          >
            {data.mark_says}
          </p>
          <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
            <Metric label="Giriş (signal high)" value={fmtUsd(data.entry)} />
            <Metric label="Stop" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R)" value={fmtUsd(data.target)} />
          </div>
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · Stoch %K {data.stoch_k} · Time stop YOK
            (trailing — Carr s.324)
          </p>
        </>
      ) : (
        <div className="grid grid-cols-4 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA20" value={fmtUsd(data.sma20)} />
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
          <Metric label="SMA200" value={fmtUsd(data.sma200)} />
          <Metric label="Stoch %K" value={data.stoch_k != null ? `${data.stoch_k}` : "—"} />
        </div>
      )}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
        {label}
      </span>
      <span className="font-mono font-semibold tabular-nums">{value}</span>
    </div>
  );
}
