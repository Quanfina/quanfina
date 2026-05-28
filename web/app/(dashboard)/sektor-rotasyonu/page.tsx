"use client";

import { TrendingUp } from "lucide-react";
import { useSectorRotation } from "@/hooks/use-sector-rotation";

/**
 * Sektör Rotasyonu sayfası — 11 SPDR sektör ETF RS rank.
 *
 * Mark/Minervini "lider sektör" disiplini: güçlü sektördeki güçlü hisse ara.
 * sector_rotation tablosu (scanner.py günlük) → /api/sector-rotation → bu sayfa.
 * RS rank 1 = en güçlü sektör. perf_1w..1y getiri ısı renkli.
 */

function pctColor(v: number | null): string {
  if (v == null) return "var(--muted-foreground)";
  if (v >= 10) return "var(--mtp-excellent)";
  if (v > 0) return "var(--mtp-good, #4B9CD3)";
  if (v > -5) return "#F59E0B";
  return "var(--mtp-danger)";
}

function fmtPct(v: number | null): string {
  if (v == null) return "—";
  return `${v > 0 ? "+" : ""}${v.toFixed(1)}%`;
}

export default function SektorRotasyonuPage() {
  const { data, isLoading, isError } = useSectorRotation();

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b">
        <h1 className="text-xl font-semibold tracking-tight flex items-center gap-2">
          <TrendingUp size={18} className="text-muted-foreground" />
          Sektör Rotasyonu
        </h1>
        <p className="text-sm text-muted-foreground">
          11 SPDR sektör ETF — RS rank (Mark/Minervini lider sektör disiplini:
          güçlü sektördeki güçlü hisse).
          {data && data.length > 0 && data[0].scan_date && (
            <span className="ml-1.5 italic">Tarama: {data[0].scan_date}</span>
          )}
        </p>
      </div>

      {/* İçerik */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {isLoading ? (
          <p className="text-sm text-muted-foreground py-12 text-center">
            Sektör rotasyonu yükleniyor...
          </p>
        ) : isError ? (
          <p className="text-sm py-12 text-center" style={{ color: "var(--mtp-danger)" }}>
            Sektör rotasyonu alınamadı (Cloud SQL erişilemez olabilir).
          </p>
        ) : !data || data.length === 0 ? (
          <p className="text-sm text-muted-foreground py-12 text-center">
            Sektör rotasyonu verisi yok (scanner henüz doldurmamış olabilir).
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="border-b text-xs text-muted-foreground uppercase tracking-wider">
                  <th className="text-left py-2 px-2 w-12">Rank</th>
                  <th className="text-left py-2 px-2">Sektör</th>
                  <th className="text-left py-2 px-2 w-16">ETF</th>
                  <th className="text-right py-2 px-2">1H</th>
                  <th className="text-right py-2 px-2">1A</th>
                  <th className="text-right py-2 px-2">3A</th>
                  <th className="text-right py-2 px-2">6A</th>
                  <th className="text-right py-2 px-2">1Y</th>
                  <th className="text-right py-2 px-2">RS Skor</th>
                </tr>
              </thead>
              <tbody>
                {data.map((s) => (
                  <tr
                    key={s.ticker}
                    className="border-b hover:bg-muted/30 transition-colors"
                  >
                    <td className="py-2 px-2">
                      <span
                        className="inline-flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold"
                        style={{
                          background:
                            s.rs_rank === 1
                              ? "color-mix(in srgb, var(--mtp-excellent) 20%, transparent)"
                              : "transparent",
                          color:
                            s.rs_rank === 1
                              ? "var(--mtp-excellent)"
                              : "var(--foreground)",
                        }}
                      >
                        {s.rs_rank ?? "—"}
                      </span>
                    </td>
                    <td className="py-2 px-2 font-medium">{s.sector_name}</td>
                    <td className="py-2 px-2 font-mono text-muted-foreground">{s.ticker}</td>
                    {([s.perf_1w, s.perf_1m, s.perf_3m, s.perf_6m, s.perf_1y] as (number | null)[]).map(
                      (v, i) => (
                        <td
                          key={i}
                          className="text-right py-2 px-2 font-mono tabular-nums"
                          style={{ color: pctColor(v) }}
                        >
                          {fmtPct(v)}
                        </td>
                      )
                    )}
                    <td className="text-right py-2 px-2 font-mono font-semibold tabular-nums">
                      {s.rs_score != null ? s.rs_score.toFixed(1) : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
