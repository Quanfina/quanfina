"use client";

import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 529 (18 Haz 2026): Carr sinyali → paper trade köprüsü.
 *
 * Detected (LONG) Carr kartının detector çıktısını (entry/stop/target/setup) AddTradeDialog'a
 * pre-fill olarak aktarır. SADECE LONG setup'larda kullanılır (trade formu LONG-yönlü:
 * R/R risk=entry−stop). onPaperTrade /hisse sayfasından gelir (dialog'u prefill ile açar).
 * onPaperTrade verilmemişse (sayfa wire etmemişse) görünmez — backward-compatible.
 */
export function CarrPaperTradeButton({
  data,
  onPaperTrade,
}: {
  data: InitialData;
  onPaperTrade?: (d: InitialData) => void;
}) {
  if (!onPaperTrade) return null;
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
