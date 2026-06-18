"use client";

import { Anchor, Minus } from "lucide-react";
import { useBullishBase } from "@/hooks/use-bullish-base";
import { fmtUsd } from "@/lib/format-currency";
import { CarrPaperTradeButton } from "@/components/stock/CarrPaperTradeButton";
import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 513 (18 Haz 2026): Carr Bullish Base Breakout paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Bullish Base Breakout" (s.291) — CONTRARIAN downtrend-sonu LONG ADAYI
 * (diğer 4 Carr setup'ı trend-takip/countertrend; bu downtrend bottom-fishing). KRİTİK:
 * kırılım BEKLENMEZ → entry=close (ilk yeşil mum, s.284,289). OBV+MACD yükseliyor + range
 * daralma. TIER-2 eyeball → quality=CANDIDATE (base tipi: rising wedge DEĞİL, s.287-288).
 */
export function BullishBaseCard({ symbol, onPaperTrade }: { symbol: string; onPaperTrade?: (d: InitialData) => void }) {
  const { data, isLoading, isError } = useBullishBase(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Bullish Base yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  // CANDIDATE (contrarian, eyeball) → amber/uyarı tonu
  const color = data.detected ? "var(--mtp-warning, #d97706)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(217,119,6,0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-bullish-base-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <Anchor size={16} /> : <Minus size={16} />}
        </span>
        Bullish Base Breakout
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Contrarian, TIER-2)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-bullish-base-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;200 bar veri — paper trade için
          güvenilmez (SMA200 gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🟡 LONG ADAYI (contrarian — göz kararı şart)" : "Aday yok"}
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
            <Metric label="Stop (%6)" value={fmtUsd(data.stop)} />
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
            Risk %{data.risk_pct} · R:R 1:{data.rr} · Kırılım beklenmez (Carr s.284) · Time stop YOK
          </p>
          <CarrPaperTradeButton
            data={{ symbol, strategy: "carr", setup_type: "bullish_base",
              entry_price: data.entry ?? undefined, plan_stop: data.stop ?? undefined,
              plan_target: data.target ?? undefined,
              plan_entry_trigger: "Carr Bullish Base (s.291) — contrarian downtrend baz, entry=close (kırılım beklenmez); göz kararı: base tipi" }}
            onPaperTrade={onPaperTrade}
          />
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA20" value={fmtUsd(data.sma20)} />
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
          <Metric label="SMA200" value={fmtUsd(data.sma200)} />
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
