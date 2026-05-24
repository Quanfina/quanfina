"use client";

import { useMarketStatus } from "@/hooks/use-market-status";
import { StageCard } from "@/components/market/StageCard";
import { HealthScoreCard } from "@/components/market/HealthScoreCard";
import { ModeSuggestionCard } from "@/components/market/ModeSuggestionCard";
import { SectorSummaryCard } from "@/components/market/SectorSummaryCard";
import { MarketStatusBadge } from "@/components/market/MarketStatusBadge";
import { MarkRegimeCard } from "@/components/market/MarkRegimeCard";

export default function PiyasaDurumuPage() {
  const { data, isLoading, isError, error } = useMarketStatus();

  if (isLoading) {
    return (
      <div className="p-6 flex items-center justify-center h-64 text-sm text-muted-foreground">
        Yükleniyor...
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div
        className="p-6 flex items-center justify-center h-64 text-sm"
        style={{ color: "var(--mtp-danger)" }}
      >
        Hata: {(error as Error)?.message ?? "Piyasa verisi alınamadı"}
      </div>
    );
  }

  return (
    <div className="p-6 flex flex-col gap-6">
      <div className="flex flex-col gap-3">
        <div className="flex flex-col gap-1">
          <h1 className="text-2xl font-semibold tracking-tight">Piyasa Durumu</h1>
          <p className="text-sm text-muted-foreground">
            SPY / QQQ / IWM stage analizi + Market Health Score
          </p>
        </div>
        {/* ABD borsa açık/kapalı + TR/ET saat + sonraki açılış (Sprint 4-bis.7) */}
        <MarketStatusBadge />
      </div>

      {/* KARAR ADAY #729 (24 May 2026): Mark Market Regime kartı (KARAR #488 4-Katman) */}
      <section className="flex flex-col gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Mark Market Regime (4-Katman × 2-Eksen)
        </h2>
        <MarkRegimeCard distributionDays={data.distribution_days} />
      </section>

      {/* Stage kartları */}
      <section className="flex flex-col gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Endeks Stage
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <StageCard symbol="SPY" stage={data.spy_stage} />
          <StageCard symbol="QQQ" stage={data.qqq_stage} />
          <StageCard symbol="IWM" stage={data.iwm_stage} />
        </div>
      </section>

      {/* Health + Mod + Sektör */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <HealthScoreCard
          score={data.market_health_score}
          label={data.market_health_label}
          vix={data.vix}
          distributionDays={data.distribution_days}
        />
        <ModeSuggestionCard mode={data.suggested_mode} />
        <SectorSummaryCard
          topSectors={data.top_sectors}
          bottomSectors={data.bottom_sectors}
        />
      </div>

      {/* Sektör Rotasyonu notu */}
      <div className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
        <p>
          <span className="font-medium">Sektör Rotasyonu detayı</span> — bu sayfada özet
          gösterilmekte. Ayrı sayfa kararı POC ADIM 5 sonunda alınacak (AÇIK KONU #49).
        </p>
      </div>
    </div>
  );
}
