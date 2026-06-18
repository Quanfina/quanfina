"use client";

import { TrendingDown, TrendingUp, Minus } from "lucide-react";
import { useMeanReversion } from "@/hooks/use-mean-reversion";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 500 (18 Haz 2026): Carr Mean Reversion paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Bonus System I" (s.356) countertrend LONG/SHORT — Quanfina çekirdek
 * frekans setup (Carr.md %80). Sinyal varsa giriş/stop(%8 cap)/hedef(SMA20); yoksa
 * BB+SMA20 bağlamı. Göstergeler SADECE BB(20,2.0)+SMA20+candle (Carr canon).
 */
export function MeanReversionCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useMeanReversion(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Mean Reversion yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const isLong = data.direction === "LONG";
  const color = data.detected
    ? isLong
      ? "var(--mtp-excellent)"
      : "var(--mtp-danger)"
    : "var(--mtp-neutral)";
  const bg = data.detected
    ? isLong
      ? "rgba(40, 167, 69, 0.10)"
      : "rgba(220, 53, 69, 0.10)"
    : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-mean-reversion-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? (
            isLong ? <TrendingUp size={16} /> : <TrendingDown size={16} />
          ) : (
            <Minus size={16} />
          )}
        </span>
        Mean Reversion
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Countertrend)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-mean-reversion-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik veri (yfinance erişilemiyor) — paper
          trade için güvenilmez.
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected
          ? isLong
            ? "🟢 LONG sinyal"
            : "🔴 SHORT sinyal"
          : "Sinyal yok"}
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
            <Metric label="Giriş" value={fmtUsd(data.entry)} />
            <Metric label={`Stop (%${data.hard_cap_pct} cap)`} value={fmtUsd(data.stop)} />
            <Metric label="Hedef (SMA20)" value={fmtUsd(data.target)} />
          </div>
          <p className="text-[10px] text-muted-foreground">
            {data.time_stop_days}-gün time stop · Range/Weak piyasada geçerli (Carr s.356/400/410)
          </p>
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="Alt BB" value={fmtUsd(data.lower_bb)} />
          <Metric label="SMA20" value={fmtUsd(data.sma20)} />
          <Metric label="Üst BB" value={fmtUsd(data.upper_bb)} />
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
