"use client";

import { ShieldAlert, ShieldCheck, TriangleAlert } from "lucide-react";
import { useStockQuote } from "@/hooks/use-stock-quote";
import { useAtrVolatility } from "@/hooks/use-atr-volatility";
import { usePivotBreakout } from "@/hooks/use-pivot-breakout";
import { useTradingMode } from "@/hooks/use-trading-mode";
import { usePortfolioValue } from "@/hooks/use-portfolio-value";
import { fmtUsd } from "@/lib/format-currency";

/**
 * P438-M2 (10 Haz 2026): Risk-Özeti kartı — Mark "Risk first" katmanı.
 *
 * /hisse UX denetimi (cross-page workflow) bulgusu: Mark'ın EN kritik ilkesi
 * (Trade Like a Wizard Sec 1 "Risk önce gelir") sayfada hiç yoktu. ATR stop
 * hesaplanıyordu ama karar akışına bağlı değildi — kullanıcı Trade Aç'a basana
 * kadar "stop nerede, kaç R" göremiyordu.
 *
 * Kural #26 (matematik uydurmama): TÜM sayılar mevcut kaynaklardan —
 *   - Giriş: canlı yfinance quote (P437/P438)
 *   - Stop: ATR 2.5× normal (useAtrVolatility — Mark TLSMW Ch 11 / Wilder canon)
 *   - 1R/2R/3R: standart Van Tharp R-multiple aritmetiği (entry-stop tabanlı)
 *   - Pivot mesafe: usePivotBreakout pivot_price
 *   - Sizing %: useTradingMode recommendedSizingPct (Mark TTLC s.187 mod disiplini)
 * Uydurma eşik YOK — 2R/3R "hedef fiyat" değil R-bazlı projeksiyon.
 */

const MODE_LABEL: Record<string, string> = {
  normal: "Normal",
  rehab: "Rehab",
  defansif: "Defansif",
  agresif: "Agresif",
};

export function RiskSummaryCard({ symbol }: { symbol: string }) {
  const { data: quote } = useStockQuote(symbol);
  const { data: atr, isLoading: atrLoading } = useAtrVolatility(symbol);
  const { data: pivot } = usePivotBreakout(symbol);
  const mode = useTradingMode();
  const { value: portfolioValue } = usePortfolioValue();

  if (atrLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Risk özeti yükleniyor...
      </div>
    );
  }

  // Giriş: canlı yfinance quote öncelik, pivot current_price fallback (P437 paten)
  const entry =
    quote?.source === "yfinance" && quote.price != null
      ? quote.price
      : pivot?.current_price ?? null;
  const stop = atr?.suggested_stop_normal ?? null;

  // Geçerli stop yoksa / stop >= giriş ise (long için imkansız) Mark risk-first ihlali
  const hasValidRisk = entry != null && stop != null && stop > 0 && stop < entry;

  if (entry == null) {
    // Canlı fiyat yoksa risk hesabı anlamsız — sessiz placeholder
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground flex items-center gap-2">
        <ShieldAlert size={14} /> Risk-Özeti: canlı fiyat alınamadı
      </div>
    );
  }

  const riskPerShare = hasValidRisk ? entry - (stop as number) : null;
  const stopPct = riskPerShare != null ? (riskPerShare / entry) * 100 : null;
  const target2R = riskPerShare != null ? entry + 2 * riskPerShare : null;
  const target3R = riskPerShare != null ? entry + 3 * riskPerShare : null;
  // Pivota mesafe (giriş − pivot)/pivot: negatif = pivot altı (henüz alım noktası değil)
  const pivotPrice = pivot?.pivot_price ?? null;
  const pivotDistPct =
    pivotPrice != null && pivotPrice > 0 ? ((entry - pivotPrice) / pivotPrice) * 100 : null;
  // Mod-bazlı pozisyon riski (Mark TTLC s.187): equity × sizing% = $ risk bütçesi
  const riskBudget =
    portfolioValue > 0 ? portfolioValue * (mode.recommendedSizingPct / 100) : null;
  const suggestedShares =
    riskBudget != null && riskPerShare != null && riskPerShare > 0
      ? Math.floor(riskBudget / riskPerShare)
      : null;

  const accent = hasValidRisk ? "var(--mtp-neutral)" : "var(--mtp-danger)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{
        background: hasValidRisk ? "rgba(75,156,211,0.07)" : "rgba(220,53,69,0.08)",
        borderColor: `${accent}55`,
      }}
    >
      {/* Başlık + mod rozeti */}
      <div className="flex items-center gap-2">
        {hasValidRisk ? (
          <ShieldCheck size={16} style={{ color: accent }} />
        ) : (
          <ShieldAlert size={16} style={{ color: accent }} />
        )}
        <h3 className="text-xs font-semibold flex-1">
          Risk-Özeti
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TTLC Sec 1 — Risk first)
          </span>
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: mode.color, color: "#fff" }}
          title={`Trade modu: ${mode.reason}`}
        >
          {mode.emoji} {MODE_LABEL[mode.mode] ?? mode.mode}
        </span>
      </div>

      {/* Giriş → Stop büyük blok */}
      <div
        className="flex items-center justify-between gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: accent }}
      >
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Giriş (canlı)
          </span>
          <span
            className="text-lg font-mono font-bold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {fmtUsd(entry)}
          </span>
        </div>
        <span className="text-muted-foreground text-sm">→</span>
        <div className="flex flex-col items-end">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Stop (ATR 2.5×)
          </span>
          <span
            className="text-lg font-mono font-bold tabular-nums"
            style={{
              color: hasValidRisk ? "var(--mtp-danger)" : "var(--muted-foreground)",
              fontFamily: "var(--font-jetbrains-mono, monospace)",
            }}
          >
            {stop != null ? fmtUsd(stop) : "—"}
          </span>
        </div>
      </div>

      {!hasValidRisk && (
        <p
          className="text-xs italic leading-relaxed px-2 py-1 flex items-center gap-1.5"
          style={{ color: "var(--mtp-danger)" }}
        >
          <TriangleAlert size={13} className="shrink-0" />
          Geçerli stop yok (stop ≥ giriş) — Mark &quot;risk first&quot; ihlali. ATR
          stop hesaplanamadı veya fiyat stop altında.
        </p>
      )}

      {hasValidRisk && (
        <>
          {/* Risk metrik grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
            <div className="flex flex-col">
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                Risk/Hisse (1R)
              </span>
              <span
                className="font-mono font-semibold tabular-nums"
                style={{ color: "var(--mtp-danger)", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                title="1R = giriş − stop (hisse başına dolar risk)"
              >
                {fmtUsd(riskPerShare as number)}
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                Stop Mesafesi
              </span>
              <span
                className="font-mono font-semibold tabular-nums"
                style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                title="(giriş − stop) / giriş — düşük = sıkı stop"
              >
                -{(stopPct as number).toFixed(1)}%
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                Pivota Mesafe
              </span>
              <span
                className="font-mono font-semibold tabular-nums"
                style={{
                  color:
                    pivotDistPct == null
                      ? "var(--muted-foreground)"
                      : pivotDistPct >= 0
                      ? "var(--mtp-excellent)"
                      : "#F59E0B",
                  fontFamily: "var(--font-jetbrains-mono, monospace)",
                }}
                title="(giriş − pivot) / pivot — negatif = pivot altı (alım noktası değil)"
              >
                {pivotDistPct != null
                  ? `${pivotDistPct >= 0 ? "+" : ""}${pivotDistPct.toFixed(1)}%`
                  : "—"}
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                Önerilen Risk
              </span>
              <span
                className="font-mono font-semibold tabular-nums"
                style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                title={`Mod ${MODE_LABEL[mode.mode] ?? mode.mode}: equity %${mode.recommendedSizingPct} risk (Mark TTLC s.187)`}
              >
                %{mode.recommendedSizingPct.toFixed(1)}
              </span>
            </div>
          </div>

          {/* R projeksiyon + sizing satırı */}
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] pt-1 border-t border-muted-foreground/15">
            <span className="text-muted-foreground">
              R projeksiyon:{" "}
              <span
                className="font-mono font-semibold"
                style={{ color: "var(--mtp-excellent)", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                title="2R = giriş + 2×(giriş−stop). Hedef fiyat değil, R-bazlı projeksiyon."
              >
                2R {fmtUsd(target2R as number)}
              </span>
              {"  ·  "}
              <span
                className="font-mono font-semibold"
                style={{ color: "var(--mtp-excellent)", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                title="3R = giriş + 3×(giriş−stop)."
              >
                3R {fmtUsd(target3R as number)}
              </span>
            </span>
            {mode.recommendedSizingPct === 0 ? (
              <span className="text-muted-foreground">
                Sizing:{" "}
                <span
                  className="font-semibold"
                  style={{ color: "var(--mtp-danger)" }}
                  title={`${MODE_LABEL[mode.mode] ?? mode.mode} mod: yeni AL pozisyonu yok (Mark TTLC s.187)`}
                >
                  yeni AL yok ({MODE_LABEL[mode.mode] ?? mode.mode})
                </span>
              </span>
            ) : (
              suggestedShares != null && riskBudget != null && (
                <span className="text-muted-foreground">
                  Sizing:{" "}
                  <span
                    className="font-mono font-semibold text-foreground"
                    style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                    title={`Mod riski $${riskBudget.toFixed(0)} / 1R $${(riskPerShare as number).toFixed(2)} = ${suggestedShares} hisse (notional portföy ayarı)`}
                  >
                    ~{suggestedShares} hisse
                  </span>
                </span>
              )
            )}
          </div>

          {/* Mark felsefe */}
          <p className="text-[10px] text-muted-foreground italic border-t border-muted-foreground/15 pt-1.5">
            Önce stop&apos;unu bil. Risk yönetimi kârdan önce gelir (Mark TTLC Sec 1).
            Bireysel risk hedefi equity %1.25-2.5 (TTLC s.143-144).
          </p>
        </>
      )}
    </div>
  );
}
