"use client";

import { TrendingDown, Minus } from "lucide-react";
import { useBlueSea } from "@/hooks/use-blue-sea";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 518 (18 Haz 2026): Carr Blue Sea Breakdown paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Blue Sea Breakdown" (s.289) — strong-bear trend-takip SHORT (Blue Sky aynası).
 * 40g yeni düşük + 52h dip DEĞİL + close>0.8×52h zirve (asimetri s.286) + OBV/MACD 40g yeni
 * düşük + kırmızı mum. Sinyal varsa giriş(signal low)/stop(%6 üstte)/hedef(2R aşağı); yoksa
 * 40g/52h düşük + 52h zirve bağlamı. SHORT → kırmızı/danger ton (Quanfina long-biased; bilgi).
 */
export function BlueSeaCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useBlueSea(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Blue Sea Breakdown yükleniyor...
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
      data-testid="hisse-blue-sea-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <TrendingDown size={16} /> : <Minus size={16} />}
        </span>
        Blue Sea Breakdown
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — SHORT, strong-bear)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-blue-sea-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;261 bar veri — paper trade için
          güvenilmez (52-hafta lookback gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🔴 SHORT sinyal (Blue Sea)" : "Sinyal yok"}
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
            <Metric label="Giriş (signal low)" value={fmtUsd(data.entry)} />
            <Metric label="Stop (%6 üstte)" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R aşağı)" value={fmtUsd(data.target)} />
          </div>
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · SADECE strong-bear · 52h zirve %20 altında
            (Carr s.283/286)
          </p>
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="40g Düşük" value={fmtUsd(data.low_40d)} />
          <Metric label="52h Dip" value={fmtUsd(data.low_260d)} />
          <Metric label="52h Zirve" value={fmtUsd(data.high_260d)} />
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
