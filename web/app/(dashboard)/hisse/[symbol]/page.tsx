"use client";

import { use } from "react";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { useStockInfo, useOhlcv } from "@/hooks/use-stock";
import dynamic from "next/dynamic";
import { StockHeader } from "@/components/stock/StockHeader";
import { ActiveStrategies } from "@/components/stock/ActiveStrategies";

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

      {/* Stock header */}
      <div className="px-6 py-4 border-b">
        <StockHeader info={info} />
      </div>

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

          {/* Active strategies sidebar */}
          <div className="w-64 shrink-0">
            <ActiveStrategies strategies={info.active_strategies} symbol={sym} />
          </div>
        </div>

        {/* Setup notes (only when notes exist) */}
        <SetupNotes strategies={info.active_strategies} />
      </div>
    </div>
  );
}
