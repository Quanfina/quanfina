"use client";

import { useMarketStatus } from "@/hooks/use-market-status";
import { StageCard } from "@/components/market/StageCard";
import { HealthScoreCard } from "@/components/market/HealthScoreCard";
import { ModeSuggestionCard } from "@/components/market/ModeSuggestionCard";
import { SectorSummaryCard } from "@/components/market/SectorSummaryCard";
import { MarketStatusBadge } from "@/components/market/MarketStatusBadge";
import { MarkRegimeCard } from "@/components/market/MarkRegimeCard";
import { PullbackReliefCard } from "@/components/market/PullbackReliefCard";
import { MarketDataMockBanner } from "@/components/market/MarketDataMockBanner";
import { ModBadge } from "@/components/mark/ModBadge";
import { Skeleton } from "@/components/ui/skeleton";

export default function PiyasaDurumuPage() {
  const { data, isLoading, isError, error } = useMarketStatus();

  // P447 (review M5 pattern): sayfa-seviye skeleton — eski "Yükleniyor..." düz
  // metni veri gelince tam-sayfa pop-in (CLS) yapıyordu. Layout-aynalı skeleton.
  if (isLoading) {
    return (
      <div className="p-6 flex flex-col gap-6">
        <div className="flex items-start justify-between gap-3">
          <div className="flex flex-col gap-2">
            <Skeleton className="h-7 w-44" />
            <Skeleton className="h-4 w-72" />
          </div>
          <Skeleton className="h-7 w-28" />
        </div>
        <Skeleton className="h-10 w-full rounded-lg" />
        <Skeleton className="h-48 w-full rounded-lg" />
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-24 w-full rounded-lg" />
          ))}
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-32 w-full rounded-lg" />
          ))}
        </div>
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
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="flex flex-col gap-1">
            <h1 className="text-2xl font-semibold tracking-tight">Piyasa Durumu</h1>
            <p className="text-sm text-muted-foreground">
              SPY / QQQ / IWM stage analizi + Market Health Score
            </p>
            {/* P447 (Kural #20 alt-bölüm): sayfa-KARAR atfı (diğer sayfalarla tutarlı) */}
            <p className="text-xs text-muted-foreground/70">
              KARAR #488 (Market Regime) + #731 (backend wire) + #733 (A/D + FTD)
            </p>
          </div>
          {/* Paket 225 (27 May 2026): Vizyon İLKE #10 — Piyasa'da mevcut mod farkındalığı
              Backend "suggested_mode" (piyasa rejimine göre öneri) vs ModBadge (Sn. Ferit
              kişisel streak-bazlı güncel mod) karşılaştırması. */}
          <ModBadge variant="compact" />
        </div>
        {/* ABD borsa açık/kapalı + TR/ET saat + sonraki açılış (Sprint 4-bis.7) */}
        <MarketStatusBadge />
      </div>

      {/* P483 (#1, Kural #28): karar-kritik şerit MOCK uyarısı — sadece canlı veri
          (index/VIX/sektör) alınamadığında görünür, aksi halde null. */}
      <MarketDataMockBanner status={data} />

      {/* P447 (review — KARAR HIZI / 5-saniye testi): üst-fold Karar Şeridi.
          Mevcut alanları (regime new_buy_allowed + allocation + suggested_mode)
          tek bakışta öne çıkarır. READ-ONLY — yeni hesap/verdict YOK; bağlayıcı
          kişisel-mod ↔ rejim combine mantığı sabaha kuyrukta (decision-critical).
          İLKE #11 objektif-ayna: durum + sebep, övgü yok. */}
      <div className="flex flex-wrap items-stretch gap-2">
        <div
          className="flex-1 min-w-[150px] rounded-lg border px-3 py-2 flex flex-col gap-0.5"
          style={{
            borderColor:
              data.mark_regime?.new_buy_allowed === false
                ? "rgba(220,53,69,0.40)"
                : "rgba(40,167,69,0.35)",
          }}
        >
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Yeni Alım
          </span>
          <span
            className="text-base font-bold"
            style={{
              color:
                data.mark_regime?.new_buy_allowed == null
                  ? "var(--muted-foreground)"
                  : data.mark_regime.new_buy_allowed
                  ? "var(--mtp-excellent)"
                  : "var(--mtp-danger)",
            }}
          >
            {data.mark_regime?.new_buy_allowed == null
              ? "—"
              : data.mark_regime.new_buy_allowed
              ? "✓ Serbest"
              : "✗ YASAK"}
          </span>
        </div>
        <div className="flex-1 min-w-[150px] rounded-lg border px-3 py-2 flex flex-col gap-0.5">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Pozisyon / Allocation
          </span>
          <span className="text-sm font-semibold">
            {data.mark_regime?.allocation ?? "—"}
          </span>
        </div>
        <div className="flex-1 min-w-[150px] rounded-lg border px-3 py-2 flex flex-col gap-0.5">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Piyasa Modu
          </span>
          <span
            className="text-base font-bold"
            style={{
              color:
                data.suggested_mode === "LONG"
                  ? "var(--mtp-excellent)"
                  : data.suggested_mode === "DEFENSIVE"
                  ? "var(--mtp-danger)"
                  : "#F59E0B",
            }}
          >
            {data.suggested_mode}
          </span>
        </div>
      </div>

      {/* KARAR ADAY #729 (24 May 2026): Mark Market Regime kartı (KARAR #488 4-Katman)
          KARAR #731 (24 May 2026): backend mark_regime tercih + client fallback */}
      <section className="flex flex-col gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Mark Market Regime (4-Katman × 2-Eksen)
        </h2>
        <MarkRegimeCard
          distributionDays={data.distribution_days}
          backendRegime={data.mark_regime}
          spyStage={data.spy_stage}
          qqqStage={data.qqq_stage}
          iwmStage={data.iwm_stage}
          marketBreadth={data.market_breadth}
          breadthDivergence={data.breadth_divergence}
          followThrough={data.follow_through}
        />
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

      {/* P570: Carr Pullback/Relief Rally oranı — piyasa rejim/aşırılık (s.280) */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <PullbackReliefCard />
      </div>

      {/* Sektör Rotasyonu notu — P446 (review): iç-geliştirme referansları
          ("POC ADIM 5 / AÇIK KONU #49") kullanıcı UI'sinden çıkarıldı (İLKE #11). */}
      <div className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
        <p>
          <span className="font-medium">Sektör Rotasyonu</span> — bu sayfada özet
          gösterilmekte. Detaylı analiz için Sektör Rotasyonu sayfasına bakın.
        </p>
      </div>
    </div>
  );
}
