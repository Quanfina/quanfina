"use client";

import { CalendarClock, AlertTriangle, Minus } from "lucide-react";
import { useEarnings } from "@/hooks/use-earnings";

/**
 * Paket 543 (20 Haz 2026): Earnings tarih farkındalığı (/hisse/[symbol]) — #843.
 *
 * Minervini "earnings crapshoot" (Minervini.md s.249): earnings'e 5 gün veya daha az kala
 * YENİ breakout alımı YAPMA + s.251 "kazanç kumarı, risk almam". Blackout (0-5 gün) →
 * kırmızı uyarı. Blackout dışı → yeşil bilgi. Veri yoksa (yfinance) gri "manuel teyit".
 * Kural #28: yfinance kaynak (tahmini olabilir) şeffaf. SOFT uyarı (block değil).
 */
export function EarningsCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useEarnings(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Earnings tarihi yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const blackout = data.has_data && data.within_blackout;
  const color = !data.has_data
    ? "var(--mtp-neutral)"
    : blackout
      ? "var(--mtp-danger)"
      : "var(--mtp-excellent)";
  const bg = !data.has_data
    ? "rgba(75, 156, 211, 0.08)"
    : blackout
      ? "rgba(220, 53, 69, 0.10)"
      : "rgba(40, 167, 69, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-earnings-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {blackout ? <AlertTriangle size={16} /> : data.has_data ? <CalendarClock size={16} /> : <Minus size={16} />}
        </span>
        Earnings Takvimi
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Minervini s.249 — {data.blackout_days}g blackout)
        </span>
      </h3>

      {data.has_data && (
        <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="Tarih" value={data.earnings_date ?? "—"} />
          <Metric
            label="Kalan gün"
            value={data.days_to_earnings != null ? `${data.days_to_earnings}g` : "—"}
            color={blackout ? "var(--mtp-danger)" : undefined}
          />
        </div>
      )}

      <p className="text-[10px] text-muted-foreground leading-relaxed" style={blackout ? { color: "var(--mtp-danger)", fontWeight: 500 } : undefined}>
        {data.mark_says}
      </p>
    </div>
  );
}

function Metric({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</span>
      <span className="font-mono font-semibold tabular-nums" style={color ? { color } : undefined}>
        {value}
      </span>
    </div>
  );
}
