"use client";

import { useMemo } from "react";
import Link from "next/link";
import { useTrades } from "@/hooks/use-trades";
import { useStockQuotes } from "@/hooks/use-stock-quote";
import { usePositionAlerts } from "@/hooks/use-position-alerts";
import { fmtUsd } from "@/lib/format-currency";
import { TrendingUp, TrendingDown, Activity, ArrowRight, AlertCircle, AlertTriangle } from "lucide-react";

/**
 * Paket 156 (26 May 2026): Dashboard Portfolio özet kartı.
 *
 * Sn. Ferit paper trading — sabah Quanfina açtığında özet bakışı:
 * - Açık pozisyon sayısı + toplam yatırılan sermaye (entry × shares)
 * - Canlı portföy değeri (current_price × shares, yfinance)
 * - Unrealized P&L $ + % (toplam)
 * - Bugünkü değişim (günlük P&L günlük değişim toplamı)
 *
 * Boş durum: Henüz açık trade yoksa "ilk trade açıldığında burada görünür".
 * MarkPyramidCard piyasa-aware öneri pateniyle DRY uyum.
 */
export function PortfolioSummaryCard() {
  const trades = useTrades();
  const openTrades = useMemo(
    () => (trades.data ?? []).filter((t) => t.status === "open"),
    [trades.data]
  );
  const openSymbols = useMemo(
    () => Array.from(new Set(openTrades.map((t) => t.symbol))),
    [openTrades]
  );
  const quoteResults = useStockQuotes(openSymbols);

  // Paket 160-161 (26 May 2026): Açık trade stop loss canlı uyarı (Mark TTLC s.131 %7 +
  // TLSMW Ch 12 plan_stop). Dashboard + Journal her ikisinde de tetiklenir — sonner ID
  // dedup spam etmez. Sn. Ferit sabah Dashboard açtığında stop'a değen pozisyonu görür.
  usePositionAlerts(openTrades, quoteResults);

  const summary = useMemo(() => {
    if (openTrades.length === 0) return null;
    const quoteMap = new Map<string, { price: number; change_pct: number | null }>();
    quoteResults.forEach((r) => {
      if (r.data) {
        quoteMap.set(r.data.symbol.toUpperCase(), {
          price: r.data.price,
          change_pct: r.data.change_pct,
        });
      }
    });

    let investedCapital = 0;     // entry_price × shares (toplam yatırılan)
    let currentValue = 0;        // current_price × shares (canlı değer)
    let todayChangeDollar = 0;   // bugünkü dolar değişim toplamı
    let coveredCount = 0;        // canlı fiyatı olan trade sayısı
    let bestSymbol = "";
    let bestPct = -Infinity;
    let worstSymbol = "";
    let worstPct = Infinity;
    // Paket 190 (26 May 2026): Sektör konsantrasyon (Mark TTLC s.85 max 25-30%)
    const sectorValues = new Map<string, number>();

    openTrades.forEach((t) => {
      const invested = t.entry_price * t.shares;
      investedCapital += invested;
      const q = quoteMap.get(t.symbol.toUpperCase());
      if (q) {
        const liveValue = q.price * t.shares;
        currentValue += liveValue;
        coveredCount += 1;
        if (q.change_pct != null) {
          todayChangeDollar += (q.change_pct / 100) * liveValue;
        }
        const tradePct = ((q.price / t.entry_price) - 1) * 100;
        if (tradePct > bestPct) { bestPct = tradePct; bestSymbol = t.symbol; }
        if (tradePct < worstPct) { worstPct = tradePct; worstSymbol = t.symbol; }
      } else {
        // Canlı yoksa entry'i değer kabul et (eksik veri)
        currentValue += invested;
      }
      // P190: Sektör konsantrasyon (Mark TTLC s.85 max 25-30% single sector)
      const sector = t.sector || "Bilinmiyor";
      const tradeValue = (t.current_price ?? t.entry_price) * t.shares;
      sectorValues.set(sector, (sectorValues.get(sector) ?? 0) + tradeValue);
    });

    const unrealizedPlDollar = currentValue - investedCapital;
    const unrealizedPlPct = investedCapital > 0
      ? (unrealizedPlDollar / investedCapital) * 100
      : 0;

    // P190: En yüksek sektör konsantrasyonu (uyarı 30% üstü)
    let topSector = "";
    let topSectorValue = 0;
    sectorValues.forEach((v, sec) => {
      if (v > topSectorValue) { topSectorValue = v; topSector = sec; }
    });
    const topSectorPct = currentValue > 0 ? (topSectorValue / currentValue) * 100 : 0;
    const sectorWarning = topSectorPct > 30; // Mark TTLC s.85

    return {
      openCount: openTrades.length,
      coveredCount,
      investedCapital,
      currentValue,
      unrealizedPlDollar,
      unrealizedPlPct,
      todayChangeDollar,
      bestSymbol,
      bestPct,
      worstSymbol,
      worstPct,
      topSector,
      topSectorPct,
      sectorWarning,
    };
  }, [openTrades, quoteResults]);

  if (trades.isLoading) {
    return (
      <div className="rounded-lg border bg-card p-4 text-sm text-muted-foreground">
        Portfolio yükleniyor...
      </div>
    );
  }

  if (!summary || summary.openCount === 0) {
    return (
      <Link
        href="/journal"
        className="rounded-lg border bg-card p-4 flex flex-col gap-1.5 hover:bg-accent transition-colors"
      >
        <span className="text-xs text-muted-foreground uppercase tracking-wider flex items-center gap-1">
          <Activity size={12} />
          Portfolio
        </span>
        <span className="text-sm text-muted-foreground">
          Henüz açık trade yok.
        </span>
        <span className="text-xs text-muted-foreground">
          İlk trade açıldığında canlı kar/zarar burada görünür.
        </span>
      </Link>
    );
  }

  const isPositive = summary.unrealizedPlDollar >= 0;
  const todayPositive = summary.todayChangeDollar >= 0;
  const TrendIcon = isPositive ? TrendingUp : TrendingDown;
  const trendColor = isPositive ? "var(--mtp-excellent)" : "var(--mtp-danger)";
  const todayColor = todayPositive ? "var(--mtp-excellent)" : "var(--mtp-danger)";

  return (
    <Link
      href="/journal"
      className="rounded-lg border bg-card p-4 flex flex-col gap-3 hover:bg-accent hover:border-accent transition-colors group"
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity size={14} className="text-muted-foreground" />
          <h3 className="text-xs font-semibold tracking-wider uppercase text-muted-foreground">
            Portfolio Canlı
          </h3>
        </div>
        <ArrowRight size={11} className="opacity-0 group-hover:opacity-60 transition-opacity" />
      </div>

      {/* Ana metrik: canlı değer + unrealized P&L */}
      <div className="flex items-baseline gap-3">
        <span
          className="text-2xl font-bold tabular-nums"
          style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {fmtUsd(summary.currentValue)}
        </span>
        <span
          className="text-sm font-semibold tabular-nums flex items-center gap-1"
          style={{ color: trendColor }}
        >
          <TrendIcon size={14} />
          {summary.unrealizedPlDollar >= 0 ? "+" : "-"}
          {fmtUsd(Math.abs(summary.unrealizedPlDollar))}
          <span className="text-xs opacity-80">
            ({summary.unrealizedPlPct >= 0 ? "+" : ""}{summary.unrealizedPlPct.toFixed(2)}%)
          </span>
        </span>
      </div>

      {/* Bugünkü değişim + en iyi/en kötü */}
      <div className="grid grid-cols-3 gap-2 text-xs">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Bugün
          </span>
          <span className="font-mono font-semibold tabular-nums" style={{ color: todayColor }}>
            {summary.todayChangeDollar >= 0 ? "+" : "-"}
            {fmtUsd(Math.abs(summary.todayChangeDollar))}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            En İyi
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ color: "var(--mtp-excellent)" }}
          >
            {summary.bestSymbol} {summary.bestPct >= 0 ? "+" : ""}
            {summary.bestPct.toFixed(1)}%
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            En Kötü
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ color: summary.worstPct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}
          >
            {summary.worstSymbol} {summary.worstPct >= 0 ? "+" : ""}
            {summary.worstPct.toFixed(1)}%
          </span>
        </div>
      </div>

      {/* Paket 190: Sektör konsantrasyon (Mark TTLC s.85 max 25-30% single sector) */}
      {summary.topSector && summary.topSectorPct > 0 && (
        <div
          className="flex items-center gap-2 text-xs px-2 py-1.5 rounded border"
          style={{
            borderColor: summary.sectorWarning
              ? "var(--mtp-danger)"
              : summary.topSectorPct > 20
              ? "#F59E0B"
              : "var(--border)",
            background: summary.sectorWarning
              ? "color-mix(in srgb, var(--mtp-danger) 8%, transparent)"
              : "transparent",
          }}
        >
          {summary.sectorWarning ? (
            <AlertTriangle size={12} style={{ color: "var(--mtp-danger)" }} />
          ) : (
            <Activity size={12} className="text-muted-foreground" />
          )}
          <span className="flex-1">
            <span className="text-muted-foreground">En yoğun sektör: </span>
            <span className="font-semibold">{summary.topSector}</span>
            <span
              className="font-mono ml-1 tabular-nums"
              style={{
                color: summary.sectorWarning ? "var(--mtp-danger)" : "inherit",
              }}
            >
              {summary.topSectorPct.toFixed(0)}%
            </span>
            {summary.sectorWarning && (
              <span className="ml-2 text-[10px]" style={{ color: "var(--mtp-danger)" }}>
                — Mark TTLC s.85: max 25-30%
              </span>
            )}
          </span>
        </div>
      )}

      {/* Footer: yatırılan sermaye + canlı veri kapsama */}
      <div className="flex items-center justify-between text-[10px] text-muted-foreground pt-2 border-t border-muted-foreground/10">
        <span>
          {summary.openCount} açık • {fmtUsd(summary.investedCapital)} yatırım
        </span>
        {summary.coveredCount < summary.openCount && (
          <span className="flex items-center gap-1" style={{ color: "var(--mtp-neutral)" }}>
            <AlertCircle size={9} />
            {summary.coveredCount}/{summary.openCount} canlı
          </span>
        )}
      </div>
    </Link>
  );
}
