"use client";

import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 529 (18 Haz 2026): Carr sinyali → paper trade köprüsü.
 *
 * Detected (LONG) Carr kartının detector çıktısını (entry/stop/target/setup) AddTradeDialog'a
 * pre-fill olarak aktarır. SADECE LONG setup'larda kullanılır (trade formu LONG-yönlü:
 * R/R risk=entry−stop). onPaperTrade /hisse sayfasından gelir (dialog'u prefill ile açar).
 * onPaperTrade verilmemişse (sayfa wire etmemişse) görünmez — backward-compatible.
 *
 * P531 (18 Haz 2026, Kural #28): isMock=true ise gizlenir. Sentetik / yetersiz-bar
 * (<60/<200/<261) detector çıktısıyla paper trade açmak yanıltıcı — kart zaten "🟡 paper
 * trade için güvenilmez" banner gösteriyor; buton da olmamalı (çelişki + dürüstlük).
 */
export function CarrPaperTradeButton({
  data,
  onPaperTrade,
  isMock,
}: {
  data: InitialData;
  onPaperTrade?: (d: InitialData) => void;
  isMock?: boolean;
}) {
  if (!onPaperTrade || isMock) return null;
  return (
    <button
      type="button"
      onClick={() => onPaperTrade(data)}
      className="text-[10px] font-semibold px-2 py-1 rounded border self-start hover:bg-accent transition-colors mt-1"
      style={{ borderColor: "var(--mtp-excellent)55", color: "var(--mtp-excellent)" }}
      title="Bu Carr sinyalini paper trade olarak aç (entry/stop/hedef plan pre-filled)"
      data-testid="carr-paper-trade-btn"
    >
      📋 Paper Trade&apos;e aktar
    </button>
  );
}
