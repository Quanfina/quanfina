"use client";

import { useMemo } from "react";
import { useTrades } from "@/hooks/use-trades";
import { computeMonthlyRba, summarizeMonthlyRba } from "@/lib/monthly-rba";

/**
 * Paket 401: Aylık RBA performans tablosu.
 *
 * Mark TTLC Sec 4 "Know the truth about your trading" canon — aylık aggregate.
 * Markets360 "Tracker" pattern uyarlaması (clean-room — kendi sütun/terim).
 *
 * Sütunlar: Ay, Trade, Win Rate, Avg Gain, Avg Loss, Net%, R-Ratio, P&L$
 * Footer: Toplam satırı (weighted aggregate).
 *
 * İLKE #11 Objektif Ayna Dil: sayısal değerler, motivasyon dili yok.
 */
export function MonthlyRbaTable() {
  const trades = useTrades();
  const monthlyRows = useMemo(
    () => computeMonthlyRba(trades.data ?? []),
    [trades.data],
  );
  const summary = useMemo(() => summarizeMonthlyRba(monthlyRows), [monthlyRows]);

  if (monthlyRows.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-4">
        <h3 className="text-sm font-semibold mb-1">Aylık Performans (RBA)</h3>
        <p className="text-xs text-muted-foreground">
          Henüz kapanan trade yok — RBA agregatı için trade kapanışı gerek
          (Mark TTLC Sec 4).
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b">
        <h3 className="text-sm font-semibold">
          Aylık Performans (RBA — Result-Based Analysis)
        </h3>
        <p className="text-[10px] text-muted-foreground">
          Mark TTLC Sec 4 &ldquo;Know the truth about your trading&rdquo; — exit_date
          bazlı aylık aggregate.
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs tabular-nums" data-testid="monthly-rba-table">
          <thead>
            <tr className="border-b text-muted-foreground">
              <th className="text-left px-3 py-2 font-medium">Ay</th>
              <th className="text-right px-2 py-2 font-medium">Trade</th>
              <th className="text-right px-2 py-2 font-medium" title="Kazanan trade oranı">Win Rate</th>
              <th className="text-right px-2 py-2 font-medium" title="Kazanan trade ortalama % (Mark RBA)">Avg Gain</th>
              <th className="text-right px-2 py-2 font-medium" title="Kaybeden trade ortalama % (Mark RBA)">Avg Loss</th>
              <th className="text-right px-2 py-2 font-medium" title="Tüm trade'lerin net ortalaması">Net %</th>
              <th className="text-right px-2 py-2 font-medium" title="Avg Gain / |Avg Loss| — Mark canon ≥1 pozitif beklenti">R-Ratio</th>
              <th className="text-right px-3 py-2 font-medium">P&amp;L $</th>
            </tr>
          </thead>
          <tbody>
            {monthlyRows.map((r) => {
              const winRateColor =
                r.winRate >= 50 ? "var(--mtp-excellent)" : "var(--muted-foreground)";
              const netColor =
                r.netPct > 0 ? "var(--mtp-excellent)" : r.netPct < 0 ? "var(--mtp-danger)" : "var(--muted-foreground)";
              const ratioColor =
                r.gainLossRatio != null && r.gainLossRatio >= 1
                  ? "var(--mtp-excellent)"
                  : r.gainLossRatio != null
                  ? "var(--mtp-danger)"
                  : "var(--muted-foreground)";
              return (
                <tr key={r.month} className="border-b border-border/50 hover:bg-accent/30">
                  <td className="px-3 py-2 font-medium">{r.monthLabel}</td>
                  <td className="text-right px-2 py-2">{r.tradeCount}</td>
                  <td className="text-right px-2 py-2" style={{ color: winRateColor }}>
                    %{r.winRate.toFixed(0)}
                  </td>
                  <td className="text-right px-2 py-2" style={{ color: "var(--mtp-excellent)" }}>
                    +{r.avgGainPct.toFixed(1)}%
                  </td>
                  <td className="text-right px-2 py-2" style={{ color: "var(--mtp-danger)" }}>
                    {r.avgLossPct.toFixed(1)}%
                  </td>
                  <td className="text-right px-2 py-2 font-semibold" style={{ color: netColor }}>
                    {r.netPct >= 0 ? "+" : ""}{r.netPct.toFixed(1)}%
                  </td>
                  <td className="text-right px-2 py-2" style={{ color: ratioColor }}>
                    {r.gainLossRatio != null ? r.gainLossRatio.toFixed(2) : "—"}
                  </td>
                  <td className="text-right px-3 py-2 font-semibold" style={{ color: netColor }}>
                    {r.totalPlDollar >= 0 ? "+" : ""}${r.totalPlDollar.toFixed(0)}
                  </td>
                </tr>
              );
            })}
          </tbody>
          {summary && (
            <tfoot>
              <tr
                className="border-t-2 font-semibold"
                style={{ background: "rgba(75,156,211,0.06)" }}
                data-testid="monthly-rba-summary-row"
              >
                <td className="px-3 py-2">{summary.monthLabel}</td>
                <td className="text-right px-2 py-2">{summary.tradeCount}</td>
                <td className="text-right px-2 py-2">%{summary.winRate.toFixed(0)}</td>
                <td className="text-right px-2 py-2" style={{ color: "var(--mtp-excellent)" }}>
                  +{summary.avgGainPct.toFixed(1)}%
                </td>
                <td className="text-right px-2 py-2" style={{ color: "var(--mtp-danger)" }}>
                  {summary.avgLossPct.toFixed(1)}%
                </td>
                <td
                  className="text-right px-2 py-2"
                  style={{ color: summary.netPct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}
                >
                  {summary.netPct >= 0 ? "+" : ""}{summary.netPct.toFixed(1)}%
                </td>
                <td className="text-right px-2 py-2">
                  {summary.gainLossRatio != null ? summary.gainLossRatio.toFixed(2) : "—"}
                </td>
                <td
                  className="text-right px-3 py-2"
                  style={{ color: summary.totalPlDollar >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}
                >
                  {summary.totalPlDollar >= 0 ? "+" : ""}${summary.totalPlDollar.toFixed(0)}
                </td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>
    </div>
  );
}
