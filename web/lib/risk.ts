/**
 * B3-01 (05 Tem 2026 — FAZ-2): Pozisyon dolar riski — backend `quanfina_math.risk_dollars`
 * BİREBİR aynası (Kural #26 — kanonlu formül birebir taşındı, yeni formül üretilmedi).
 *
 * Kök neden: 3 UI site (OpenPositionsRiskPanel, QuickSummaryBar, CloseTradeDialog)
 * inline `(entry - plan_stop) * shares` kullanıyordu — LONG-only. SHORT (invest_type=2,
 * stop>entry) → negatif/yanlış risk. Backend `sign + max-floor` ile doğru; frontend'de
 * kayıptı. Tek kaynak (DRY, H#A1) → 3 site buna bağlanır.
 *
 * Backend referans (quanfina_math.py:104-109):
 *   sign = 1 if invest_type == LONG else -1
 *   return max((entry - stop) * sign * shares, 0.0)
 *
 * invest_type: 1=LONG (default) / 2=SHORT (Migration 013 / P539). undefined → LONG.
 */
export function riskDollar(
  entry: number,
  planStop: number,
  shares: number,
  investType?: 1 | 2,
): number {
  const sign = investType === 2 ? -1 : 1; // SHORT: (entry-stop)*-1 = (stop-entry)
  return Math.max((entry - planStop) * sign * shares, 0);
}
