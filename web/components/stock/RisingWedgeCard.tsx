"use client";

import { TrendingDown, Minus } from "lucide-react";
import { useRisingWedge } from "@/hooks/use-rising-wedge";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 522 (18 Haz 2026): Carr Rising Wedge Breakdown paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Rising Wedge Breakdown" (Böl.19) — uptrend-sonu yükselen kama kırılımı SHORT
 * ADAYI. SMA50 uptrend + range daralma (kama) + MACD/OBV bearish divergence (40g düşüyor) +
 * kırmızı. ENTRY=close (OBV trendline kırılımı sonrası, eyeball). TIER-2 → quality=CANDIDATE.
 * SHORT → kırmızı/danger ton (Quanfina long-biased; bilgi amaçlı). Carr catalog SON setup.
 */
export function RisingWedgeCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useRisingWedge(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Rising Wedge yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const color = data.detected ? "var(--mtp-danger)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(220, 53, 69, 0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-rising-wedge-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <TrendingDown size={16} /> : <Minus size={16} />}
        </span>
        Rising Wedge Breakdown
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — SHORT, TIER-2)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-rising-wedge-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;90 bar veri — paper trade için
          güvenilmez.
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🔴 SHORT ADAYI (göz kararı şart)" : "Aday yok"}
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
            <Metric label="Stop (%6 üstte)" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R aşağı)" value={fmtUsd(data.target)} />
          </div>
          {data.eyeball_checks.length > 0 && (
            <ul className="text-[10px] text-muted-foreground list-disc pl-4 space-y-0.5 pt-1">
              {data.eyeball_checks.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          )}
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · MACD/OBV bearish divergence · Bullish/Range
          </p>
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
          <Metric label="MACD" value={data.macd != null ? `${data.macd}` : "—"} />
          <Metric label="OBV" value={data.obv != null ? `${Math.round(data.obv / 1000)}K` : "—"} />
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
