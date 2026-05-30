"use client";

import { useMemo } from "react";
import { ShieldAlert, TrendingDown } from "lucide-react";
import { useTrades } from "@/hooks/use-trades";
import { usePortfolioValue } from "@/hooks/use-portfolio-value";
import type { Trade } from "@/types/trade";

/**
 * KARAR ADAY #455 — Risk-Merkez UI: Açık Pozisyon Risk Dağılımı.
 *
 * Markets360 sentezi (Drawdown 100 + SL Trail 58 frequency) + Mark canon.
 * Her açık pozisyonun dolar riski (entry - plan_stop) × shares + % portföy +
 * unrealized R. Toplam portföy riski. Mark TTLC risk disiplini:
 *   - Bireysel trade equity risk %1.25-2.5 (Mindset risk-03, TTLC s.143-144)
 *   - %2.5 üstü → sarı uyarı, plan_stop yok → kırmızı (Mark "risk first")
 *
 * PortfolioSummaryCard unrealized P&L + sektör konsantrasyon hesaplar;
 * bu panel RISK (entry-stop) dağılımını gösterir (DRY — farklı metrik).
 */

interface PositionRisk {
  // P399: id eklendi -> React duplicate key fix. Sn. Ferit pyramiding ile
  // ayni sembol+strateji icin birden fazla acik trade acabilir (Mark Pilot/
  // Standart/Full tier'lari) -> "symbol-strategy" key duplicate riski vardi.
  // trade.id PRIMARY KEY (web_trades SERIAL) -> garanti unique.
  id: number;
  symbol: string;
  strategy: string;
  dollarRisk: number | null;     // (entry - plan_stop) × shares
  pctRisk: number | null;        // dollarRisk / portfolio
  unrealizedR: number | null;    // (current - entry) / (entry - stop)
  hasStop: boolean;
}

interface Props {
  /** Portföy değeri — opsiyonel override; verilmezse usePortfolioValue() hook'tan (P400). */
  portfolioValue?: number;
}

export function OpenPositionsRiskPanel({ portfolioValue: portfolioValueProp }: Props) {
  // P400: prop override öncelikli, yoksa localStorage hook'undan kullanıcı ayarı
  const { value: storedPortfolioValue } = usePortfolioValue();
  const portfolioValue = portfolioValueProp ?? storedPortfolioValue;
  const trades = useTrades();

  const { positions, totalDollarRisk, totalPctRisk, noStopCount } = useMemo(() => {
    const open = (trades.data ?? []).filter((t: Trade) => t.status === "open");
    const rows: PositionRisk[] = open.map((t) => {
      const hasStop = t.plan_stop != null && t.plan_stop > 0;
      const dollarRisk = hasStop
        ? (t.entry_price - (t.plan_stop as number)) * t.shares
        : null;
      const pctRisk =
        dollarRisk != null && portfolioValue > 0
          ? (dollarRisk / portfolioValue) * 100
          : null;
      // Unrealized R: (current - entry) / (entry - stop)
      let unrealizedR: number | null = null;
      if (hasStop && t.current_price != null) {
        const riskPerShare = t.entry_price - (t.plan_stop as number);
        if (riskPerShare > 0) {
          unrealizedR = (t.current_price - t.entry_price) / riskPerShare;
        }
      }
      return {
        id: t.id,  // P399: trade PRIMARY KEY -> React unique key garanti
        symbol: t.symbol,
        strategy: t.strategy,
        dollarRisk,
        pctRisk,
        unrealizedR,
        hasStop,
      };
    });
    const totalDollarRisk = rows.reduce((s, r) => s + (r.dollarRisk ?? 0), 0);
    const totalPctRisk =
      portfolioValue > 0 ? (totalDollarRisk / portfolioValue) * 100 : 0;
    const noStopCount = rows.filter((r) => !r.hasStop).length;
    return { positions: rows, totalDollarRisk, totalPctRisk, noStopCount };
  }, [trades.data, portfolioValue]);

  if (trades.isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Açık pozisyon riskleri yükleniyor...
      </div>
    );
  }

  if (positions.length === 0) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        <span className="font-semibold">Açık Pozisyon Riski: </span>
        Açık pozisyon yok — risk taşınmıyor.
      </div>
    );
  }

  // Mark risk disiplini renk: bireysel %2.5 üstü sarı, plan_stop yok kırmızı
  function riskColor(pct: number | null, hasStop: boolean): string {
    if (!hasStop) return "var(--mtp-danger)";
    if (pct == null) return "var(--muted-foreground)";
    if (pct > 2.5) return "#F59E0B"; // Mark TTLC s.143-144 üst sınır aşımı
    return "var(--mtp-excellent)";
  }

  return (
    <div className="rounded-lg border p-3 flex flex-col gap-3">
      {/* Başlık + toplam */}
      <div className="flex items-center justify-between gap-2 pb-2 border-b">
        <h3 className="text-sm font-semibold flex items-center gap-1.5">
          <ShieldAlert size={15} className="text-muted-foreground" />
          Açık Pozisyon Riski
          <span className="text-[10px] font-normal text-muted-foreground italic">
            ({positions.length} pozisyon)
          </span>
        </h3>
        <div className="text-right">
          <div
            className="text-sm font-bold tabular-nums"
            style={{
              color: totalPctRisk > 6 ? "var(--mtp-danger)" : "var(--foreground)",
            }}
          >
            ${totalDollarRisk.toFixed(0)}
          </div>
          <div className="text-[10px] text-muted-foreground">
            Toplam risk %{totalPctRisk.toFixed(2)}
          </div>
        </div>
      </div>

      {/* plan_stop eksik uyarı */}
      {noStopCount > 0 && (
        <div
          className="text-[11px] px-2 py-1.5 rounded flex items-center gap-1.5"
          style={{
            background: "rgba(220,53,69,0.10)",
            color: "var(--mtp-danger)",
          }}
        >
          <TrendingDown size={13} className="shrink-0" />
          {noStopCount} pozisyonda stop loss yok — Mark &quot;risk first&quot; ihlali (TTLC Sec 2-3)
        </div>
      )}

      {/* Pozisyon listesi */}
      <ul className="flex flex-col gap-1.5">
        {positions.map((p) => {
          const color = riskColor(p.pctRisk, p.hasStop);
          return (
            <li key={p.id} className="flex items-center gap-2 text-xs">
              <span className="font-mono font-semibold w-14 shrink-0">{p.symbol}</span>
              {/* Risk barı (görsel ısı) */}
              <div className="flex-1 h-2 rounded-full bg-muted/40 overflow-hidden">
                <div
                  className="h-full rounded-full"
                  style={{
                    width: p.pctRisk != null ? `${Math.min(p.pctRisk / 5 * 100, 100)}%` : "100%",
                    background: color,
                  }}
                />
              </div>
              <span className="font-mono tabular-nums w-16 text-right" style={{ color }}>
                {p.dollarRisk != null ? `$${p.dollarRisk.toFixed(0)}` : "stop yok"}
              </span>
              <span className="font-mono tabular-nums w-12 text-right text-muted-foreground">
                {p.pctRisk != null ? `%${p.pctRisk.toFixed(1)}` : "—"}
              </span>
              <span
                className="font-mono tabular-nums w-14 text-right"
                style={{
                  color:
                    p.unrealizedR == null
                      ? "var(--muted-foreground)"
                      : p.unrealizedR >= 0
                      ? "var(--mtp-excellent)"
                      : "var(--mtp-danger)",
                }}
                title="Unrealized R-Multiple (current vs entry/stop)"
              >
                {p.unrealizedR != null
                  ? `${p.unrealizedR >= 0 ? "+" : ""}${p.unrealizedR.toFixed(1)}R`
                  : "—"}
              </span>
            </li>
          );
        })}
      </ul>

      {/* Mark felsefe notu */}
      <p className="text-[10px] text-muted-foreground italic border-t pt-2">
        Bireysel risk hedefi %1.25-2.5 equity (Mark TTLC s.143-144 / Mindset risk-03).
        %2.5 üstü sarı, stop yok kırmızı.
      </p>
    </div>
  );
}
