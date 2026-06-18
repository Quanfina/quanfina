"use client";

import { Activity, Minus } from "lucide-react";
import { useBullishDivergence } from "@/hooks/use-bullish-divergence";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 515 (18 Haz 2026): Carr Bullish Divergence paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Bullish Divergence" (s.258) — uptrend-dip LONG ADAYI: fiyat lower low yaparken
 * 6 göstergeden (MACD line/hist, Stoch %K, RSI(5), CCI(20), OBV) 2+'si higher low. TIER-2
 * eyeball → quality=CANDIDATE. Aday varsa giriş(close)/stop(sell-off dibi)/hedef(2R) + diverge
 * gösterge listesi + eyeball checklist; yoksa SMA + kaç gösterge diverge bağlamı.
 */
export function BullishDivergenceCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useBullishDivergence(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Bullish Divergence yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const color = data.detected ? "var(--mtp-warning, #d97706)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(217,119,6,0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-bullish-divergence-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <Activity size={16} /> : <Minus size={16} />}
        </span>
        Bullish Divergence
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Uptrend-dip, TIER-2)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-bullish-divergence-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;200 bar veri — paper trade için
          güvenilmez (SMA200 gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🟡 LONG ADAYI (göz kararı şart)" : "Aday yok"}
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
            <Metric label="Giriş (close)" value={fmtUsd(data.entry)} />
            <Metric label="Stop" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R)" value={fmtUsd(data.target)} />
          </div>
          {data.eyeball_checks.length > 0 && (
            <ul className="text-[10px] text-muted-foreground list-disc pl-4 space-y-0.5 pt-1">
              {data.eyeball_checks.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          )}
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · {data.divergence_count}/6 gösterge diverge ·
            uzun tutma (Carr s.252)
          </p>
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
          <Metric label="SMA200" value={fmtUsd(data.sma200)} />
          <Metric label="Diverge" value={`${data.divergence_count}/6`} />
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
