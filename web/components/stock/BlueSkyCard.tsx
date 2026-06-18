"use client";

import { Rocket, Minus } from "lucide-react";
import { useBlueSky } from "@/hooks/use-blue-sky";
import { fmtUsd } from "@/lib/format-currency";
import { CarrPaperTradeButton } from "@/components/stock/CarrPaperTradeButton";
import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 508 (18 Haz 2026): Carr Blue Sky Breakout paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Blue Sky Breakout" (s.264-265 entry + s.324-325 exit) — STRONG+WEAK_BULL
 * trend-takip LONG breakout. SMA YOK (s.261); 40g yeni yüksek + 52h zirve ALTI + OBV/MACD
 * yeni 40g yüksek + green mum. Sinyal varsa giriş(signal high)/stop(%6 Ch22)/hedef(2R);
 * yoksa 40g/52h yüksek + OBV/MACD bağlamı. Çift danışma (Carr NotebookLM) doğrulandı.
 */
export function BlueSkyCard({ symbol, onPaperTrade }: { symbol: string; onPaperTrade?: (d: InitialData) => void }) {
  const { data, isLoading, isError } = useBlueSky(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Blue Sky Breakout yükleniyor...
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
      data-testid="hisse-blue-sky-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <Rocket size={16} /> : <Minus size={16} />}
        </span>
        Blue Sky Breakout
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Trend-takip)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-blue-sky-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;261 bar veri — paper trade için
          güvenilmez (52-hafta lookback gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🟢 LONG sinyal (Blue Sky)" : "Sinyal yok"}
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
            <Metric label="Stop (%6)" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R)" value={fmtUsd(data.target)} />
          </div>
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · 40g yeni yüksek + OBV/MACD teyit · Time
            stop YOK (trailing — Carr s.324)
          </p>
          <CarrPaperTradeButton
            data={{ symbol, strategy: "carr", setup_type: "blue_sky",
              entry_price: data.entry ?? undefined, plan_stop: data.stop ?? undefined,
              plan_target: data.target ?? undefined,
              plan_entry_trigger: "Carr Blue Sky (s.264) — 40g yeni yüksek breakout + OBV/MACD teyit, ertesi gün signal high üstü" }}
            onPaperTrade={onPaperTrade}
            isMock={data.is_mock}
          />
        </>
      ) : (
        <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="40g Yüksek" value={fmtUsd(data.high_40d)} />
          <Metric label="52h Yüksek" value={fmtUsd(data.high_260d)} />
          <Metric label="52h Dip" value={fmtUsd(data.low_260d)} />
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
