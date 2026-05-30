"use client";

import { useMemo } from "react";
import { useTrades } from "@/hooks/use-trades";
import { computeReturnDistribution } from "@/lib/return-distribution";

/**
 * Paket 406: Return Aralıkları Dağılımı + Van Tharp Beklenti (E).
 *
 * Markets360 "DRMA Distribution" pattern uyarlaması (clean-room — kendi
 * isim/değer/yapı). "DRMA" YASAK tescilli; "Beklenti (E)" Van Tharp jenerik.
 *
 * Mark TTLC Sec 4 RBA + Van Tharp Trade Your Way to Financial Freedom canon:
 *   E = WinRate × AvgGain - LossRate × |AvgLoss|
 *   >0 = pozitif beklenti (sistem karlı), <0 = sistemli zarar
 *
 * Sn. Ferit hangi % aralığında trade'leri yoğun ve hangi aralık en karlı görür.
 * İLKE #11 Objektif Ayna Dil: sayı + birim, motivasyon dili YOK.
 */
export function ReturnDistributionTable() {
  const trades = useTrades();
  const summary = useMemo(
    () => computeReturnDistribution(trades.data ?? []),
    [trades.data],
  );

  if (summary.totalClosedTrades === 0) {
    return (
      <div className="rounded-lg border bg-card p-4">
        <h3 className="text-sm font-semibold mb-1">Return Aralıkları Dağılımı</h3>
        <p className="text-xs text-muted-foreground">
          Henüz kapalı trade yok — dağılım hesabı için trade kapanışı gerek.
        </p>
      </div>
    );
  }

  const expectancyColor =
    summary.expectancy > 0
      ? "var(--mtp-excellent)"
      : summary.expectancy < 0
      ? "var(--mtp-danger)"
      : "var(--muted-foreground)";

  // Max trade sayısı — bar genişliği oranlamak için
  const maxTrades = Math.max(1, ...summary.rows.map((r) => r.totalTrades));

  return (
    <div className="rounded-lg border bg-card overflow-hidden" data-testid="return-distribution-table">
      <div className="px-4 py-3 border-b flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold">Return Aralıkları Dağılımı</h3>
          <p className="text-[10px] text-muted-foreground">
            Mark TTLC Sec 4 RBA + Van Tharp Beklenti (E) — |pl_pct| bracket'lara
            dağılım. Sn. Ferit hangi aralıkta trade yoğun + en karlı görür.
          </p>
        </div>
        <div className="flex flex-col items-end">
          <span className="text-[10px] text-muted-foreground">Beklenti (E)</span>
          <span
            className="text-lg font-bold tabular-nums"
            style={{ color: expectancyColor }}
            title="Van Tharp E = WinRate × AvgGain - LossRate × |AvgLoss|. >0 pozitif beklenti."
          >
            {summary.expectancy >= 0 ? "+" : ""}{summary.expectancy.toFixed(2)}%
          </span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs tabular-nums">
          <thead>
            <tr className="border-b text-muted-foreground">
              <th className="text-left px-3 py-2 font-medium w-24">Aralık</th>
              <th className="text-center px-2 py-2 font-medium w-32">Dağılım</th>
              <th className="text-right px-2 py-2 font-medium">Kazanan</th>
              <th className="text-right px-2 py-2 font-medium">Kaybeden</th>
              <th className="text-right px-2 py-2 font-medium">Win %</th>
              <th className="text-right px-2 py-2 font-medium">Net %</th>
              <th className="text-right px-3 py-2 font-medium">Net $</th>
            </tr>
          </thead>
          <tbody>
            {summary.rows.map((r) => {
              const emptyRow = r.totalTrades === 0;
              const winBarPct =
                r.totalTrades > 0 ? (r.wins / r.totalTrades) * 100 : 0;
              const lossBarPct = 100 - winBarPct;
              const widthPct = (r.totalTrades / maxTrades) * 100;
              return (
                <tr
                  key={r.bracket.label}
                  className="border-b border-border/50 hover:bg-accent/30"
                  style={{ opacity: emptyRow ? 0.4 : 1 }}
                >
                  <td className="px-3 py-2 font-medium">{r.bracket.label}</td>
                  <td className="px-2 py-2">
                    {/* Inline mini bar — kazanan/kaybeden oran görsel */}
                    <div
                      className="h-3 rounded-full bg-muted/40 overflow-hidden flex"
                      style={{ width: `${Math.max(widthPct, 4)}%`, minWidth: 24 }}
                      title={`${r.totalTrades} trade — ${winBarPct.toFixed(0)}% kazanan`}
                    >
                      {r.wins > 0 && (
                        <div
                          className="h-full"
                          style={{ width: `${winBarPct}%`, background: "var(--mtp-excellent)" }}
                        />
                      )}
                      {r.losses > 0 && (
                        <div
                          className="h-full"
                          style={{ width: `${lossBarPct}%`, background: "var(--mtp-danger)" }}
                        />
                      )}
                    </div>
                  </td>
                  <td
                    className="text-right px-2 py-2"
                    style={{ color: r.wins > 0 ? "var(--mtp-excellent)" : "var(--muted-foreground)" }}
                  >
                    {r.wins}
                  </td>
                  <td
                    className="text-right px-2 py-2"
                    style={{ color: r.losses > 0 ? "var(--mtp-danger)" : "var(--muted-foreground)" }}
                  >
                    {r.losses}
                  </td>
                  <td
                    className="text-right px-2 py-2"
                    style={{
                      color: emptyRow ? "var(--muted-foreground)" : r.winRate >= 50 ? "var(--mtp-excellent)" : "var(--muted-foreground)",
                    }}
                  >
                    {emptyRow ? "—" : `%${r.winRate.toFixed(0)}`}
                  </td>
                  <td
                    className="text-right px-2 py-2 font-semibold"
                    style={{
                      color: emptyRow
                        ? "var(--muted-foreground)"
                        : r.netPctSum > 0
                        ? "var(--mtp-excellent)"
                        : r.netPctSum < 0
                        ? "var(--mtp-danger)"
                        : "var(--muted-foreground)",
                    }}
                  >
                    {emptyRow ? "—" : `${r.netPctSum >= 0 ? "+" : ""}${r.netPctSum.toFixed(1)}%`}
                  </td>
                  <td
                    className="text-right px-3 py-2 font-semibold"
                    style={{
                      color: emptyRow
                        ? "var(--muted-foreground)"
                        : r.netDollarSum > 0
                        ? "var(--mtp-excellent)"
                        : r.netDollarSum < 0
                        ? "var(--mtp-danger)"
                        : "var(--muted-foreground)",
                    }}
                  >
                    {emptyRow ? "—" : `${r.netDollarSum >= 0 ? "+" : ""}$${r.netDollarSum.toFixed(0)}`}
                  </td>
                </tr>
              );
            })}
          </tbody>
          <tfoot>
            <tr
              className="border-t-2 font-semibold"
              style={{ background: "rgba(75,156,211,0.06)" }}
              data-testid="return-distribution-summary-row"
            >
              <td className="px-3 py-2">Toplam</td>
              <td className="px-2 py-2 text-center text-muted-foreground text-[10px]">
                {summary.totalClosedTrades} trade
              </td>
              <td className="text-right px-2 py-2" style={{ color: "var(--mtp-excellent)" }}>
                {summary.rows.reduce((s, r) => s + r.wins, 0)}
              </td>
              <td className="text-right px-2 py-2" style={{ color: "var(--mtp-danger)" }}>
                {summary.rows.reduce((s, r) => s + r.losses, 0)}
              </td>
              <td className="text-right px-2 py-2">%{summary.overallWinRate.toFixed(0)}</td>
              <td className="text-right px-2 py-2 text-muted-foreground text-[10px]">
                Avg: {summary.avgGainPct >= 0 ? "+" : ""}{summary.avgGainPct.toFixed(1)}% / {summary.avgLossPct.toFixed(1)}%
              </td>
              <td className="text-right px-3 py-2" style={{ color: expectancyColor }}>
                E: {summary.expectancy >= 0 ? "+" : ""}{summary.expectancy.toFixed(2)}%
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
