"use client";

import { use, useState } from "react";
import Link from "next/link";
import { ArrowLeft, Plus } from "lucide-react";
import { useStockInfo, useOhlcv } from "@/hooks/use-stock";
import dynamic from "next/dynamic";
import { StockHeader } from "@/components/stock/StockHeader";
import { ActiveStrategies } from "@/components/stock/ActiveStrategies";
import { CarrStageCard } from "@/components/stock/CarrStageCard";
import { PivotBreakoutCard } from "@/components/stock/PivotBreakoutCard";
import { OverheadSupplyCard } from "@/components/stock/OverheadSupplyCard";
import { ClimaxRunCard } from "@/components/stock/ClimaxRunCard";
import { RsRatingCard } from "@/components/stock/RsRatingCard";
import { AtrVolatilityCard } from "@/components/stock/AtrVolatilityCard";
import { StageTransitionCard } from "@/components/stock/StageTransitionCard";
import { MarkProfileBar } from "@/components/stock/MarkProfileBar";
import { RelativeVolumeCard } from "@/components/stock/RelativeVolumeCard";
import { BreakoutQualityCard } from "@/components/stock/BreakoutQualityCard";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { ModBadge } from "@/components/mark/ModBadge";
import { PreTradeChecklist } from "@/components/journal/PreTradeChecklist";
import { useRsRating } from "@/hooks/use-rs-rating";
import { usePivotBreakout } from "@/hooks/use-pivot-breakout";
import { useTradingMode, isNewAlBlocked } from "@/hooks/use-trading-mode";
import { useCarrStage } from "@/hooks/use-carr-stage";
import { useClimaxRun } from "@/hooks/use-climax-run";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { Button } from "@/components/ui/button";
import { todayLocalISO } from "@/lib/format-date";
import { toast } from "sonner";

const PriceChart = dynamic(
  () => import("@/components/stock/PriceChart").then((m) => ({ default: m.PriceChart })),
  {
    ssr: false,
    loading: () => (
      <div
        className="flex items-center justify-center border rounded-lg text-sm text-muted-foreground"
        style={{ height: 470 }}
      >
        Grafik yükleniyor...
      </div>
    ),
  }
);
import { SetupNotes } from "@/components/stock/SetupNotes";

export default function HissePage({
  params,
}: {
  params: Promise<{ symbol: string }>;
}) {
  const { symbol } = use(params);
  const sym = symbol.toUpperCase();

  const { data: info, isLoading: infoLoading, isError: infoError } = useStockInfo(sym);
  const { data: ohlcv, isLoading: ohlcvLoading } = useOhlcv(sym);
  // KARAR #733 alt-paket (Paket 39): hisse Stage 4 ise banner'da somut uyarı
  const { data: carrStage } = useCarrStage(sym);
  const isStage4 = carrStage?.stage === 4;
  // KARAR #733 alt-paket (Paket 104): /hisse sayfasında climax_top tetiği MarkRegimeBanner'a
  const { data: climaxData } = useClimaxRun(sym);
  const isClimaxTop = climaxData?.category === "CLIMAX_TOP";
  // Paket 241 (27 May 2026): Pre-flight Mark canon mini-versiyon (sembol seviyesi)
  const { data: rsData } = useRsRating(sym);
  const { data: pivotData } = usePivotBreakout(sym);
  // Paket 256 (27 May 2026): Defansif modda Trade Aç buton görsel disabled
  // (AddTradeDialog override mevcut — yine de tıklanırsa dialog açılır)
  const tradingMode = useTradingMode();
  const alBlocked = isNewAlBlocked(tradingMode.mode);

  // KARAR #733 alt-paket (Paket 46): Hisse detay sayfasından doğrudan trade aç.
  // Sn. Ferit Watchlist/Hisse Tarama'dan hisseye girdiğinde direkt buradan
  // trade açabilsin. Sinyaller AL butonu pateni — pre-fill ile.
  const [tradeOpen, setTradeOpen] = useState(false);

  function handleTradeClick() {
    // Stage 4 ise warning toast — KALICI İLKE #4 Mark "UZAK DUR" canon.
    // Dialog yine açılır, Sn. Ferit'in kararı (mekanik karar Kural #23 saklı).
    if (isStage4) {
      toast.warning(
        `${sym} Carr Stage 4 (Declining) — Mark felsefe: UZAK DUR. Yine de trade açacak mısın?`,
        { duration: 6000 }
      );
    }
    setTradeOpen(true);
  }

  const isLoading = infoLoading || ohlcvLoading;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64 text-sm text-muted-foreground">
        Yükleniyor...
      </div>
    );
  }

  if (infoError || !info) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-4">
        <p className="text-sm" style={{ color: "var(--mtp-danger)" }}>
          <strong>{sym}</strong> hissesi bulunamadı
        </p>
        <Link
          href="/watchlist"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeft size={14} />
          İzleme Listesi&apos;ne dön
        </Link>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Breadcrumb */}
      <div className="px-6 py-2 border-b">
        <Link
          href="/watchlist"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeft size={14} />
          İzleme Listesi
        </Link>
      </div>

      {/* KARAR #733 alt-paket (Paket 39): Mark Regime banner + bu hissenin
          Stage 4 olup olmadığı sayım payı (isStage4 ? 1 : 0). hideOnHealthy
          default — HEALTHY iken sadece bu hisse Stage 4 olursa banner kalır. */}
      <MarkRegimeBanner
        stage4Count={isStage4 ? 1 : 0}
        totalCount={1}
        climaxTopCount={isClimaxTop ? 1 : 0}
      />

      {/* Paket 241 (27 May 2026): Pre-flight Mark canon checklist (sembol seviyesi).
          Stage + RS + Pivot canlı bağlı; plan alanları yok (sembol sayfa, dialog değil).
          Sn. Ferit Trade Aç tıklamadan ÖNCE Mark canon profili tek bakışta görür. */}
      <div className="px-6 py-3 border-b">
        <PreTradeChecklist
          symbol={sym}
          stage={carrStage?.stage}
          rsRating={rsData?.rs_rating ?? null}
          pivotPass={pivotData?.status === "CONFIRMED" || pivotData?.status === "WEAK"}
          compact
        />
      </div>

      {/* Stock header + Trade Aç buton (KARAR #733 alt-paket Paket 46) */}
      <div className="px-6 py-4 border-b flex items-start justify-between gap-3 flex-wrap">
        <div className="flex-1 min-w-0">
          <StockHeader info={info} />
        </div>
        <div className="flex items-center gap-3 shrink-0">
          {/* Paket 224 (27 May 2026): Vizyon İLKE #10 — Hisse detayında mod farkındalığı
              Sn. Ferit "Trade Aç" tıklamadan ÖNCE mod rozeti görür (Rehab/Defansif uyarı). */}
          <ModBadge variant="compact" />
          <Button
            size="sm"
            onClick={handleTradeClick}
            style={{
              // Paket 256: Defansif modda gri renk (AddTradeDialog override mevcut)
              background: alBlocked
                ? "#9CA3AF"
                : isStage4
                ? "var(--mtp-danger)"
                : "#28A745",
              color: "#fff",
            }}
            title={
              alBlocked
                ? "DEFANSİF mod aktif — Mark TTLC s.187 yeni AL BLOK. AddTradeDialog'da override checkbox gerektirir."
                : isStage4
                ? "Carr Stage 4 — Mark UZAK DUR uyarisi (yine de devam edebilirsin)"
                : "Bu hisseye trade aç (form pre-fill ile)"
            }
          >
            <Plus size={14} className="mr-1.5" />
            Trade Aç
          </Button>
        </div>
      </div>

      {/* KARAR #733 alt-paket (Paket 125): Mark Profili kompakt rozet bar
          — 10 sidebar kart yerine yatay özet, 1 bakışta Mark canon profili */}
      <MarkProfileBar symbol={sym} />

      {/* Main content */}
      <div className="flex-1 px-6 py-4 flex flex-col gap-4 overflow-auto">
        <div className="flex gap-4 items-start">
          {/* Chart — takes remaining space */}
          <div className="flex-1 min-w-0">
            {ohlcv && ohlcv.length > 0 ? (
              <PriceChart data={ohlcv} />
            ) : (
              <div className="flex items-center justify-center h-64 text-sm text-muted-foreground border rounded-lg">
                Grafik verisi yok
              </div>
            )}
          </div>

          {/* Active strategies sidebar + Carr Stage (KARAR #733 P32) */}
          <div className="w-64 shrink-0 flex flex-col gap-3">
            <ActiveStrategies strategies={info.active_strategies} symbol={sym} />
            <CarrStageCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 72): Pivot Breakout Card */}
            <PivotBreakoutCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 78): Overhead Supply Card */}
            <OverheadSupplyCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 88): Climax Run Card — Mark TLSMW Ch 9 */}
            <ClimaxRunCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 94): RS Rating Card — Mark TLSMW Ch 3-5 / IBD */}
            <RsRatingCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 103): ATR Volatility Card — Mark TLSMW Ch 11 / Wilder */}
            <AtrVolatilityCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 122): Stage Transition — Mark TLSMW Ch 4 / Weinstein */}
            <StageTransitionCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 133): Relative Volume — Mark TLSMW Ch 6 */}
            <RelativeVolumeCard symbol={sym} />
            {/* KARAR #733 alt-paket (Paket 134): Breakout Quality — Mark TLSMW Ch 10 */}
            <BreakoutQualityCard symbol={sym} />
          </div>
        </div>

        {/* Setup notes (only when notes exist) */}
        <SetupNotes strategies={info.active_strategies} />
      </div>

      {/* KARAR #733 alt-paket (Paket 46): AddTradeDialog — sembol + fiyat pre-fill.
          Mevcut aktif stratejilerin ilkini default seçer (Mark+Carr çift strateji
          aktivse "minervini" tercih). Plan/Stop/Hedef Sn. Ferit elle doldurur. */}
      <AddTradeDialog
        open={tradeOpen}
        onOpenChange={setTradeOpen}
        initialData={{
          symbol: sym,
          strategy: info.active_strategies?.[0]?.strategy ?? "minervini",
          entry_date: todayLocalISO(), // Paket 356: yerel tarih (UTC off-by-one fix)
          entry_price: info.price,
        }}
      />
    </div>
  );
}
