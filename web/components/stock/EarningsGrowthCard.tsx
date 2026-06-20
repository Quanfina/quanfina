"use client";

import { TrendingUp, Minus } from "lucide-react";
import { useEarningsGrowth } from "@/hooks/use-earnings-growth";

/**
 * Paket 569 (20 Haz 2026): Earnings Acceleration kartı (/hisse) — CANSLIM 'C' (#75 derin fundamental).
 *
 * Minervini CANSLIM-C: çeyreklik satış+kazanç YoY büyümesi ≥%25 + HIZLANIYOR. Scan tek-çeyrek
 * eps_qoq'tan derin (yfinance quarterly income statement, çok-çeyrek YoY). Veri yoksa "veri yok"
 * (Kural #28 — yfinance erişilemezse dürüst, uydurma yok).
 */
export function EarningsGrowthCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useEarningsGrowth(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Kazanç ivmesi yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const color = !data.available
    ? "var(--mtp-neutral)"
    : data.both_pass
      ? "var(--mtp-excellent)"
      : "var(--mtp-warning, #d97706)";
  const bg = !data.available
    ? "rgba(75, 156, 211, 0.08)"
    : data.both_pass
      ? "rgba(40, 167, 69, 0.10)"
      : "rgba(217,119,6,0.10)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-earnings-growth-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>{data.available ? <TrendingUp size={16} /> : <Minus size={16} />}</span>
        Kazanç İvmesi (CANSLIM C)
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Minervini — YoY ≥%25 + hızlanma)
        </span>
      </h3>

      {data.available ? (
        <>
          <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
            <Metric
              label="Satış YoY"
              value={data.revenue_yoy_pct != null ? `%${data.revenue_yoy_pct.toFixed(0)}` : "n/m"}
              accel={data.revenue_accelerating}
            />
            <Metric
              label="Kazanç YoY"
              value={data.earnings_yoy_pct != null ? `%${data.earnings_yoy_pct.toFixed(0)}` : "n/m"}
              accel={data.earnings_accelerating}
            />
          </div>
          <p className="text-[10px] text-muted-foreground leading-relaxed">{data.mark_says}</p>
        </>
      ) : (
        <p className="text-[10px] text-muted-foreground leading-relaxed">{data.mark_says}</p>
      )}
    </div>
  );
}

function Metric({ label, value, accel }: { label: string; value: string; accel: boolean }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</span>
      <span className="font-mono font-semibold tabular-nums flex items-center gap-1">
        {value}
        {accel && (
          <span title="Hızlanıyor — son YoY > önceki YoY" style={{ color: "var(--mtp-excellent)" }}>
            ▲
          </span>
        )}
      </span>
    </div>
  );
}
