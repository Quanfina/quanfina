"use client";

import { useEffect, useState } from "react";
import { useRiskAdvisor, type RiskAdvisorResponse } from "@/hooks/use-risk-advisor";
import { useRbaMetrics } from "@/hooks/use-rba-metrics";

/**
 * Sprint 4-bis.7 Faz 1 B paket — Mark Risk Advisor UI komponenti
 * KARAR ADAY #914 + #969 + #970 (Vizyon v22.00 tescili)
 * Trade form'a entegre: entry_price + shares değişince Mark'a danışır.
 *
 * KARAR ADAY #727 (24 May 2026): RBA gerçek veri bağlantısı.
 * Önceki placeholder (avg_gain=undefined) yerine, Mark Dynamic Stop hesabı
 * (avg_gain/2) için kullanıcının kapanmış trade'lerinden istatistik gelir.
 *
 * Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
 */

interface Props {
  entryPrice: string;
  shares: string;
  // KARAR #727: RBA filtre için strategy (Minervini/Carr istatistikleri ayrı analiz)
  strategy?: string;
  // Opsiyonel — gelecek extension
  portfolioValue?: number;        // varsayılan $100K (placeholder Sn. Ferit ayarı gelene kadar)
  totalPositions?: number;        // mevcut açık trade sayısı
  isBestName?: boolean;
}

const DEFAULT_PORTFOLIO_VALUE = 100000;

export function MarkRiskAdvisor({
  entryPrice,
  shares,
  strategy,
  portfolioValue = DEFAULT_PORTFOLIO_VALUE,
  totalPositions = 0,
  isBestName = false,
}: Props) {
  const [data, setData] = useState<RiskAdvisorResponse | null>(null);
  const { mutate, isPending, error } = useRiskAdvisor();
  // KARAR #727 — RBA gerçek veri (compute_dynamic_stop için)
  const { data: rbaData } = useRbaMetrics({ strategy });

  const ep = parseFloat(entryPrice);
  const sh = parseInt(shares);
  const positionValue = !isNaN(ep) && !isNaN(sh) && ep > 0 && sh > 0 ? ep * sh : 0;
  const positionPctActual = portfolioValue > 0 ? (positionValue / portfolioValue) * 100 : 0;

  // RBA istatistik anlamlı ise (>= 1 kapanmış trade) Mark Dynamic Stop'a beslenir
  const rbaPayload =
    rbaData && rbaData.metrics.num_trades >= 1
      ? {
          avg_gain_pct: rbaData.metrics.avg_gain_pct,
          avg_loss_pct: rbaData.metrics.avg_loss_pct,
          num_trades: rbaData.metrics.num_trades,
        }
      : {};

  // Auto-fetch on input change (debounced via mutation)
  useEffect(() => {
    if (positionValue <= 0) return;
    // Reverse-engineer max_stop_pct so position size matches what user typed
    // risk_dollars = portfolio * (target_risk_pct/100)
    // position_dollars = risk_dollars / (max_stop_pct/100)
    // For advisory display: use Mark default %2 risk + %7 stop, then show 6-rule check
    // KARAR #727: RBA varsa Mark Dynamic Stop avg_gain/2 hesabı kullanır
    mutate(
      {
        portfolio_value: portfolioValue,
        target_risk_pct: 2.0,
        max_stop_pct: 7.0,
        total_positions: totalPositions,
        is_best_name: isBestName,
        ...rbaPayload,
      },
      {
        onSuccess: (resp) => setData(resp),
      }
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entryPrice, shares, portfolioValue, totalPositions, isBestName, mutate, positionValue, rbaData?.metrics.num_trades]);

  if (positionValue <= 0) {
    return (
      <div className="rounded-md border border-dashed border-muted-foreground/30 p-3 text-xs text-muted-foreground">
        Entry fiyatı ve adet girilince Mark Risk Danışmanı aktif olacak.
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-xs text-destructive">
        Risk advisor bağlantı hatası: {String(error)}
      </div>
    );
  }

  // Mark'a göre kullanıcının girdiği position'un yüzdesi vs önerilen
  const userPositionTier =
    positionPctActual <= 20 ? "pilot_buy" :
    positionPctActual <= 25 ? "optimal" :
    positionPctActual <= 50 ? "aggressive" : "over_max";

  const tierColor = {
    pilot_buy: "text-blue-600 dark:text-blue-400",
    optimal: "text-green-600 dark:text-green-400",
    aggressive: "text-yellow-600 dark:text-yellow-400",
    over_max: "text-red-600 dark:text-red-400",
  }[userPositionTier];

  const tierLabel = {
    pilot_buy: "Pilot Buy",
    optimal: "Optimal (%20-25)",
    aggressive: "Aggressive",
    over_max: "🔴 MAX %50 aşıldı",
  }[userPositionTier];

  return (
    <div className="rounded-md border border-blue-200 dark:border-blue-900 bg-blue-50/50 dark:bg-blue-950/30 p-3 space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-blue-900 dark:text-blue-100">
          📚 Mark Risk Danışmanı
          <span className="ml-2 text-[10px] font-normal text-muted-foreground">
            (KARAR #914 #969 #970 — Vizyon v22.00)
          </span>
        </span>
        {isPending && <span className="text-[10px] text-muted-foreground">Hesaplanıyor...</span>}
      </div>

      {/* Aktif pozisyon değerlendirmesi */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div>
          <div className="text-muted-foreground">Pozisyon Değeri</div>
          <div className="font-mono font-semibold">${positionValue.toFixed(0)}</div>
        </div>
        <div>
          <div className="text-muted-foreground">Portfolio %</div>
          <div className={`font-mono font-semibold ${tierColor}`}>
            %{positionPctActual.toFixed(2)} — {tierLabel}
          </div>
        </div>
      </div>

      {data && (
        <>
          {/* Mark KESIN önerileri */}
          <div className="border-t border-blue-200/50 dark:border-blue-900/50 pt-2 grid grid-cols-2 gap-2 text-xs">
            <div>
              <div className="text-muted-foreground">Mark Önerilen Stop</div>
              <div className="font-mono font-semibold">
                %{data.recommended_stop_pct.toFixed(1)}
                {data.stop_absolute_cap_applied && (
                  <span className="ml-1 text-[10px] text-red-600">cap</span>
                )}
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                {data.stop_method === "rba_based" ? "RBA tabanlı" : "Fallback"}
              </div>
            </div>
            <div>
              <div className="text-muted-foreground">6-Kural Kontrolü</div>
              <div className="font-mono font-semibold">
                {data.six_rule_pass_count}/6
                {data.six_rule_all_pass ? (
                  <span className="ml-1 text-green-600">✅</span>
                ) : data.six_rule_critical_violations.length > 0 ? (
                  <span className="ml-1 text-red-600">🔴</span>
                ) : (
                  <span className="ml-1 text-yellow-600">⚠️</span>
                )}
              </div>
              {data.six_rule_critical_violations.length > 0 && (
                <div className="text-[10px] text-red-600 mt-0.5">
                  Critical: {data.six_rule_critical_violations.join(", ")}
                </div>
              )}
            </div>
          </div>

          {/* 6-Rule detay */}
          <details className="text-xs">
            <summary className="cursor-pointer text-blue-700 dark:text-blue-300 hover:underline">
              Mark 6-Kural detayı (TTLC s.144)
            </summary>
            <ul className="mt-2 space-y-1 text-[11px]">
              {data.six_rules.map((r) => (
                <li
                  key={r.rule_no}
                  className={
                    r.critical
                      ? "text-red-700 dark:text-red-400"
                      : r.passed
                      ? "text-muted-foreground"
                      : "text-yellow-700 dark:text-yellow-400"
                  }
                >
                  <span className="font-semibold">#{r.rule_no} {r.rule}:</span> {r.message}
                </li>
              ))}
            </ul>
          </details>

          {data.sizing_warnings.length > 0 && (
            <div className="border-t border-yellow-200/50 pt-2 text-[11px] text-yellow-700 dark:text-yellow-400">
              ⚠️ {data.sizing_warnings.join(" · ")}
            </div>
          )}
        </>
      )}

      <div className="border-t border-blue-200/30 pt-1.5 text-[10px] text-muted-foreground italic">
        Mark KESIN: stop ≤ %{data?.mark_constants.stop_absolute_cap_pct ?? 10} · risk %{data?.mark_constants.equity_risk_min_pct ?? 1.25}-{data?.mark_constants.equity_risk_max_pct ?? 2.5} equity · 4-12 stok optimal
      </div>
    </div>
  );
}
