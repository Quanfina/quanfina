"use client";

import { useMemo } from "react";
import Link from "next/link";
import { ExternalLink } from "lucide-react";
import { useOhlcv, useStockInfo } from "@/hooks/use-stock";
import { useStockQuote } from "@/hooks/use-stock-quote";
import { PriceChart } from "@/components/stock/PriceChart";
import { StatusBadge } from "@/components/watchlist/StatusBadge";
import { ConsensusBadge } from "@/components/watchlist/ConsensusBadge";
import type { WatchlistRow } from "@/types/watchlist";

/**
 * Paket 407: Watchlist split-pane view (sol kompakt liste + sağ chart).
 *
 * Yabancı platform Charting sayfa "master-detail" pattern uyarlaması
 * (clean-room — rakip platformun tescilli üst stripe'lar + alert + AI chat
 * widget'ları YASAK). Quanfina'nın mevcut lightweight-charts + use-stock
 * altyapısı reuse — kendi Mark canon (Stage + RS + Setup) mini rozet.
 *
 * Sn. Ferit liste/chart geçişinde state kaybetmeden hızlı tarama yapar.
 * Detaylı 11 compute kartı için "Tam Detay →" button → /hisse/[symbol].
 *
 * Mark "Risk first" disiplini: sağ panelde Stage + RS + Pivot + Volume rozeti
 * ön planda. İLKE #11 Objektif Ayna Dil — sayı + birim, motivasyon yok.
 */
interface Props {
  rows: WatchlistRow[];
  selectedSymbol: string | null;
  onSelectSymbol: (symbol: string) => void;
}

export function WatchlistSplitView({ rows, selectedSymbol, onSelectSymbol }: Props) {
  // İlk satırı default seç (Sn. Ferit symbol gönderdiyse onu)
  const effectiveSymbol = useMemo(() => {
    if (selectedSymbol) {
      const found = rows.find((r) => r.symbol === selectedSymbol);
      if (found) return found.symbol;
    }
    return rows[0]?.symbol ?? null;
  }, [rows, selectedSymbol]);

  return (
    <div
      className="flex h-full overflow-hidden border rounded-lg"
      data-testid="watchlist-split-view"
    >
      {/* SOL — Kompakt liste */}
      <div className="w-2/5 min-w-[280px] overflow-y-auto border-r">
        <table className="w-full text-xs tabular-nums">
          <thead className="sticky top-0 bg-card z-10 border-b">
            <tr className="text-[10px] text-muted-foreground uppercase tracking-wider">
              <th className="text-left px-2 py-1.5 font-medium">Sembol</th>
              <th className="text-left px-1 py-1.5 font-medium">Durum</th>
              <th className="text-right px-1 py-1.5 font-medium">Fiyat</th>
              <th className="text-right px-1 py-1.5 font-medium">RS</th>
              <th className="text-right px-2 py-1.5 font-medium">Pivot</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={5} className="text-center text-xs text-muted-foreground py-6">
                  Sembol yok
                </td>
              </tr>
            )}
            {rows.map((row) => {
              const key = `${row.symbol}-${row.strategy}`;
              const isActive = row.symbol === effectiveSymbol;
              return (
                <tr
                  key={key}
                  onClick={() => onSelectSymbol(row.symbol)}
                  className="cursor-pointer hover:bg-accent/40 transition-colors border-b border-border/30"
                  style={{
                    background: isActive ? "rgba(75,156,211,0.10)" : undefined,
                    fontWeight: isActive ? 600 : undefined,
                  }}
                  data-testid={`watchlist-split-row-${row.symbol}`}
                >
                  <td className="px-2 py-1.5 font-medium">
                    <div className="flex items-center gap-1.5">
                      <span>{row.symbol}</span>
                      {row.consensus_count != null && row.consensus_count > 1 && (
                        <ConsensusBadge value={row.consensus_count} />
                      )}
                    </div>
                    <span className="text-[9px] text-muted-foreground uppercase">
                      {row.strategy}
                    </span>
                  </td>
                  <td className="px-1 py-1.5">
                    <StatusBadge value={row.status} />
                  </td>
                  <td className="text-right px-1 py-1.5">
                    {row.price != null ? `$${row.price.toFixed(2)}` : "—"}
                  </td>
                  <td className="text-right px-1 py-1.5">
                    {row.rs_rating ?? "—"}
                  </td>
                  <td className="text-right px-2 py-1.5 text-muted-foreground">
                    {row.pivot_price != null ? `$${row.pivot_price.toFixed(2)}` : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* SAĞ — Chart paneli */}
      <div className="flex-1 min-w-0 overflow-y-auto">
        {effectiveSymbol ? (
          <SplitChartPanel symbol={effectiveSymbol} />
        ) : (
          <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
            Sembol seç
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * Sağ panel — seçili sembolün chart + üst metrik bar + Mark canon rozet özet.
 */
function SplitChartPanel({ symbol }: { symbol: string }) {
  const ohlcv = useOhlcv(symbol);
  const info = useStockInfo(symbol);
  const quote = useStockQuote(symbol);

  const bars = ohlcv.data ?? [];
  const lastBar = bars[bars.length - 1];
  const prevBar = bars[bars.length - 2];

  // Günlük O/H/L/C + Volume (PriceChart üstüne özet)
  const change = quote.data?.change_dollar ?? (lastBar && prevBar ? lastBar.close - prevBar.close : null);
  const changePct = quote.data?.change_pct ?? (lastBar && prevBar && prevBar.close ? ((lastBar.close - prevBar.close) / prevBar.close) * 100 : null);
  const changeColor =
    change != null && change > 0 ? "var(--mtp-excellent)" : change != null && change < 0 ? "var(--mtp-danger)" : "var(--muted-foreground)";

  return (
    <div className="flex flex-col h-full" data-testid={`split-chart-panel-${symbol}`}>
      {/* Üst başlık + Tam Detay link */}
      <div className="px-4 py-2.5 border-b flex items-center justify-between gap-3 sticky top-0 bg-card z-10">
        <div className="flex items-baseline gap-3 min-w-0">
          <h2 className="text-lg font-bold tracking-tight">{symbol}</h2>
          {info.data?.name && (
            <span className="text-xs text-muted-foreground truncate">{info.data.name}</span>
          )}
        </div>
        <Link
          href={`/hisse/${symbol}`}
          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          data-testid="watchlist-split-detail-link"
        >
          <span>Tam Detay</span>
          <ExternalLink size={11} />
        </Link>
      </div>

      {/* Üst metrik şeridi (OHLC + Volume) */}
      {lastBar && (
        <div className="px-4 py-2 border-b flex items-center gap-4 text-xs tabular-nums">
          <span>
            <span className="text-muted-foreground">Son: </span>
            <span className="font-semibold">${lastBar.close.toFixed(2)}</span>
          </span>
          {change != null && changePct != null && (
            <span className="font-semibold" style={{ color: changeColor }}>
              {change >= 0 ? "+" : ""}${change.toFixed(2)} ({changePct >= 0 ? "+" : ""}{changePct.toFixed(2)}%)
            </span>
          )}
          <span className="text-muted-foreground">
            O <span className="text-foreground">${lastBar.open.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            H <span className="text-foreground">${lastBar.high.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            L <span className="text-foreground">${lastBar.low.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            Vol <span className="text-foreground">{(lastBar.volume / 1_000_000).toFixed(2)}M</span>
          </span>
        </div>
      )}

      {/* Mark canon mini rozet şeridi (Stage + RS + Setup) */}
      {info.data && (
        <div className="px-4 py-2 border-b flex items-center gap-3 text-[11px]">
          {info.data.stage != null && (
            <span
              className="px-2 py-0.5 rounded-full font-semibold tabular-nums"
              style={{
                background: info.data.stage === 2 ? "rgba(40,167,69,0.12)" : "rgba(150,150,150,0.12)",
                color: info.data.stage === 2 ? "var(--mtp-excellent)" : "var(--muted-foreground)",
              }}
              title="Carr Stage Analysis — Stage 2 = Advancing"
            >
              Stage {info.data.stage}
            </span>
          )}
          {info.data.rs_rating != null && (
            <span
              className="px-2 py-0.5 rounded-full font-semibold tabular-nums"
              style={{
                background: info.data.rs_rating >= 90 ? "rgba(40,167,69,0.12)" : "rgba(150,150,150,0.12)",
                color: info.data.rs_rating >= 90 ? "var(--mtp-excellent)" : "var(--muted-foreground)",
              }}
              title="IBD Relative Strength rating (1-99)"
            >
              RS {info.data.rs_rating}
            </span>
          )}
          {info.data.setup_type && (
            <span className="px-2 py-0.5 rounded-full bg-muted/40 text-muted-foreground">
              {info.data.setup_type}
            </span>
          )}
        </div>
      )}

      {/* Chart */}
      <div className="flex-1 min-h-[400px]">
        {ohlcv.isLoading && (
          <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
            Grafik yükleniyor...
          </div>
        )}
        {ohlcv.isError && (
          <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
            Grafik alınamadı
          </div>
        )}
        {!ohlcv.isLoading && !ohlcv.isError && bars.length > 0 && (
          <PriceChart data={bars} />
        )}
      </div>
    </div>
  );
}
