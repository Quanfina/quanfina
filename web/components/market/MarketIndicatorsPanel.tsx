"use client";

import { Activity } from "lucide-react";
import { useExtraIndicators } from "@/hooks/use-extra-indicators";
import { HelpTip } from "@/components/shared/HelpTip";

/**
 * P425 (31 May 2026): Ek Piyasa Göstergeleri paneli — Ana Sayfa altı ayrı bölüm.
 *
 * Derin tarama (notebook/analizler/Market_Health_Metodoloji_Arastirma.md) F4/F5/F7
 * bulgularının kodlaması. Her gösterge (?) tooltip ile açıklamalı (Sn. Ferit
 * talimat). Kural #28: gerçek veri yetersizse "veri birikiyor (N/M gün)" — MOCK
 * sayı YOK. NAAIM/AAII/Put-Call harici ücretli feed olmadığı için EKLENMEDİ.
 */

// Kanon tooltip açıklamaları (Kural #26 — kaynak atıflı)
const TIPS = {
  faber:
    "Faber GTAA (SSRN 2007): SPY aylık kapanış 10-aylık ortalamanın ÜSTÜNDE ise " +
    "YATIRIMLI, ALTINDA ise NAKİT. Tek girdi: fiyat (≈200-günlük MA). Basit, mekanik " +
    "trend-takip timing kuralı. Quanfina'nın çok-girdili modelinin sade karşıtı.",
  mcclellan:
    "McClellan Oscillator: (19-gün EMA − 39-gün EMA) of (yükselen − düşen hisse). " +
    "Pozitif = breadth momentum yukarı, negatif = aşağı; sıfır geçişi momentum dönüşü. " +
    "Kaynak: mcoscillator.com. Breadth = Quanfina tarama evreni (~120 sembol) gerçek " +
    "fiyat geçmişinden günlük advance/decline (NYSE-geneli değil, evren-içi).",
  zweig:
    "Zweig Breadth Thrust: 10-günlük [yükselen/(yükselen+düşen)] EMA. Oran 10 gün içinde " +
    "0.40 altından 0.615 üstüne çıkarsa NADİR güçlü boğa başlangıcı sinyali. Kaynak: " +
    "Martin Zweig 'Winning on Wall Street'. Şu an bulunduğu bölge gösterilir.",
} as const;

function SignalChip({ text, color }: { text: string; color: string }) {
  return (
    <span
      className="text-xs font-semibold px-2 py-0.5 rounded-full tabular-nums"
      style={{ background: `color-mix(in srgb, ${color} 15%, transparent)`, color }}
    >
      {text}
    </span>
  );
}

function PendingChip({ have, need }: { have: number; need: number }) {
  return (
    <span
      className="text-xs font-medium px-2 py-0.5 rounded-full"
      style={{ background: "color-mix(in srgb, var(--muted-foreground) 12%, transparent)", color: "var(--muted-foreground)" }}
      title="Gerçek veri birikiyor — Kural #28: MOCK sayı gösterilmez"
    >
      veri birikiyor ({have}/{need} gün)
    </span>
  );
}

export function MarketIndicatorsPanel() {
  const { data, isLoading, isError } = useExtraIndicators();

  return (
    <div className="rounded-lg border bg-card p-4 flex flex-col gap-3" data-testid="market-indicators-panel">
      <div className="flex items-center gap-2">
        <Activity size={16} className="text-muted-foreground" />
        <h2 className="text-sm font-semibold">Ek Piyasa Göstergeleri</h2>
        <span className="text-[11px] text-muted-foreground">
          (derin araştırma sentezi — Faber · McClellan · Zweig)
        </span>
      </div>

      {isLoading && (
        <div className="text-xs text-muted-foreground py-3">Yükleniyor...</div>
      )}
      {isError && (
        <div className="text-xs text-muted-foreground py-3">Göstergeler alınamadı (backend?).</div>
      )}

      {data && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {/* 1. Faber 10-Ay SMA Timing */}
          <div className="rounded-md border bg-background/40 p-3 flex flex-col gap-2">
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-medium">Faber 10-Ay SMA</span>
              <HelpTip text={TIPS.faber} />
            </div>
            {data.faber.data_sufficient ? (
              <>
                <SignalChip
                  text={data.faber.signal === "INVESTED" ? "YATIRIMLI" : "NAKİT"}
                  color={data.faber.signal === "INVESTED" ? "var(--mtp-excellent)" : "var(--mtp-danger)"}
                />
                <div className="text-[11px] text-muted-foreground tabular-nums">
                  Fiyat ${data.faber.price?.toFixed(2)} vs 10-ay SMA ${data.faber.sma_10mo?.toFixed(2)}
                  {data.faber.pct_vs_sma != null && (
                    <span style={{ color: data.faber.pct_vs_sma >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
                      {" "}({data.faber.pct_vs_sma >= 0 ? "+" : ""}{data.faber.pct_vs_sma.toFixed(1)}%)
                    </span>
                  )}
                </div>
              </>
            ) : (
              <PendingChip have={data.faber.days_have} need={data.faber.days_needed} />
            )}
          </div>

          {/* 2. McClellan Oscillator */}
          <div className="rounded-md border bg-background/40 p-3 flex flex-col gap-2">
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-medium">McClellan Osilatör</span>
              <HelpTip text={TIPS.mcclellan} />
            </div>
            {data.mcclellan.data_sufficient ? (
              <>
                <SignalChip
                  text={`${data.mcclellan.value} · ${data.mcclellan.signal === "BULLISH" ? "POZİTİF" : "NEGATİF"}`}
                  color={data.mcclellan.signal === "BULLISH" ? "var(--mtp-excellent)" : "var(--mtp-danger)"}
                />
                <div className="text-[11px] text-muted-foreground">Breadth momentum (19g−39g EMA)</div>
              </>
            ) : (
              <PendingChip have={data.mcclellan.days_have} need={data.mcclellan.days_needed} />
            )}
          </div>

          {/* 3. Zweig Breadth Thrust */}
          <div className="rounded-md border bg-background/40 p-3 flex flex-col gap-2">
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-medium">Zweig Breadth Thrust</span>
              <HelpTip text={TIPS.zweig} />
            </div>
            {data.zweig.data_sufficient ? (
              <>
                <SignalChip
                  text={
                    data.zweig.zone === "THRUST_ZONE" ? "THRUST ↑"
                    : data.zweig.zone === "OVERSOLD" ? "AŞIRI SATIM"
                    : "NÖTR"
                  }
                  color={
                    data.zweig.zone === "THRUST_ZONE" ? "var(--mtp-excellent)"
                    : data.zweig.zone === "OVERSOLD" ? "#F59E0B"
                    : "var(--mtp-neutral)"
                  }
                />
                <div className="text-[11px] text-muted-foreground tabular-nums">
                  10g A/D EMA: {data.zweig.ema_ratio?.toFixed(3)} (eşik 0.40→0.615)
                </div>
              </>
            ) : (
              <PendingChip have={data.zweig.days_have} need={data.zweig.days_needed} />
            )}
          </div>
        </div>
      )}

      {/* Kural #28 dürüstlük notu + breadth kaynağı şeffaflığı (P426) */}
      <p className="text-[10px] text-muted-foreground/70 leading-relaxed">
        Gerçek veri: SPY fiyat (yfinance) +{" "}
        {data?.breadth_source === "backfill"
          ? "breadth = tarama evreni ~120 sembol gerçek fiyat geçmişinden günlük advance/decline (P426 backfill)"
          : data?.breadth_source === "scans"
          ? "breadth = günlük tarama A/D sayımı"
          : "breadth verisi"}
        . NAAIM/AAII/Put-Call gibi harici sentiment göstergeleri ücretli feed
        gerektirdiği için eklenmedi (MOCK sayı gösterilmez — Kural #28).
      </p>
    </div>
  );
}
