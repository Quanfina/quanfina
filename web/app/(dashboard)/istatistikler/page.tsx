"use client";

import { useMemo } from "react";
import { BarChart3 } from "lucide-react";
import { useTrades } from "@/hooks/use-trades";
import { computeTradeStats, GRADE_ORDER } from "@/lib/trade-stats";
import { BrandonExpectancyCard } from "@/components/journal/BrandonExpectancyCard";
import { RbaSummaryCard } from "@/components/journal/RbaSummaryCard";

/**
 * İstatistikler sayfası — trade performans dashboard (Streamlit'ten taşındı).
 *
 * Mark RBA disiplini (TTLC Sec 4 "Know the truth"). Win rate + P&L + grade
 * dağılımı + Brandon expectancy + RBA özet. İLKE #11 Objektif Ayna Dil:
 * sayılar konuşur, övgü yok.
 */

const GRADE_COLORS: Record<string, string> = {
  "A+": "#28A745", "A": "#90EE90", "B": "#4B9CD3",
  "C": "#FFDA0D", "D": "#FF5733", "F": "#D70040",
};

function StatCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="rounded-lg border bg-card p-4 flex flex-col gap-1">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</span>
      <span className="text-2xl font-bold tabular-nums" style={{ color: color ?? "var(--foreground)" }}>
        {value}
      </span>
    </div>
  );
}

export default function IstatistiklerPage() {
  const trades = useTrades();
  const stats = useMemo(() => computeTradeStats(trades.data ?? []), [trades.data]);

  const maxGrade = Math.max(1, ...Object.values(stats.gradeDistribution));

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b">
        <h1 className="text-xl font-semibold tracking-tight flex items-center gap-2">
          <BarChart3 size={18} className="text-muted-foreground" />
          İstatistikler
        </h1>
        <p className="text-sm text-muted-foreground">
          Trade performans özeti — Mark RBA disiplini (TTLC Sec 4: &ldquo;Know the truth
          about your trading&rdquo;). Sayılar konuşur.
        </p>
      </div>

      <div className="flex-1 overflow-auto px-6 py-4 flex flex-col gap-4">
        {trades.isLoading ? (
          <p className="text-sm text-muted-foreground py-12 text-center">İstatistikler yükleniyor...</p>
        ) : (
          <>
            {/* Özet metrikler */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <StatCard label="Kapalı Trade" value={String(stats.totalClosed)} />
              <StatCard label="Açık Pozisyon" value={String(stats.totalOpen)} />
              <StatCard
                label="Win Rate"
                value={`%${stats.winRate.toFixed(1)}`}
                color={stats.winRate >= 50 ? "var(--mtp-excellent)" : "#F59E0B"}
              />
              <StatCard
                label="Toplam P&L"
                value={`${stats.totalPlDollar >= 0 ? "+" : ""}$${stats.totalPlDollar.toFixed(0)}`}
                color={stats.totalPlDollar >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)"}
              />
              <StatCard label="Kazanan / Kaybeden" value={`${stats.winners} / ${stats.losers}`} />
              <StatCard
                label="Ortalama P&L %"
                value={`${stats.avgPlPct >= 0 ? "+" : ""}${stats.avgPlPct.toFixed(1)}%`}
                color={stats.avgPlPct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)"}
              />
              <StatCard
                label="En İyi Trade"
                value={stats.bestPlPct != null ? `+${stats.bestPlPct.toFixed(1)}%` : "—"}
                color="var(--mtp-excellent)"
              />
              <StatCard
                label="En Kötü Trade"
                value={stats.worstPlPct != null ? `${stats.worstPlPct.toFixed(1)}%` : "—"}
                color="var(--mtp-danger)"
              />
            </div>

            {/* Grade dağılımı */}
            <div className="rounded-lg border bg-card p-4">
              <h3 className="text-sm font-semibold mb-3">Grade Dağılımı (TradeGrader)</h3>
              {stats.totalClosed === 0 ? (
                <p className="text-xs text-muted-foreground">Henüz kapalı trade yok.</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {GRADE_ORDER.map((g) => {
                    const count = stats.gradeDistribution[g] ?? 0;
                    return (
                      <div key={g} className="flex items-center gap-2 text-xs">
                        <span className="w-6 font-bold" style={{ color: GRADE_COLORS[g] }}>{g}</span>
                        <div className="flex-1 h-3 rounded-full bg-muted/40 overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{ width: `${(count / maxGrade) * 100}%`, background: GRADE_COLORS[g] }}
                          />
                        </div>
                        <span className="w-8 text-right tabular-nums">{count}</span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Mevcut kart reuse: Brandon Expectancy + RBA özet (DRY) */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <BrandonExpectancyCard />
              <RbaSummaryCard variant="full" />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
