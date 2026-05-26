"use client";

import { useState, useMemo, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { useAddTrade, useSetupTypes } from "@/hooks/use-trades";
import { calcPL, fmtPLDollar, fmtPLPct } from "@/lib/math";
import type { TradeCreate, TradeGrade, ExitReason, TradeStatus, SignalSource, TimeHorizon } from "@/types/trade";
import { GRADE_OPTIONS, EXIT_REASON_LABELS, SIGNAL_SOURCE_LABELS, SIGNAL_SOURCE_DESCRIPTIONS, TIME_HORIZON_LABELS, TIME_HORIZON_DESCRIPTIONS } from "@/types/trade";
import { MarkRiskAdvisor } from "./MarkRiskAdvisor";
import { MarkPyramidCard } from "./MarkPyramidCard";
import { PreTradeChecklist } from "./PreTradeChecklist";
import { isReadTodayAny } from "@/lib/mindset-read-state";
import Link from "next/link";
import { Quote, AlertTriangle } from "lucide-react";
import { useCarrStage } from "@/hooks/use-carr-stage";
import { usePivotBreakout } from "@/hooks/use-pivot-breakout";
import { useClimaxRun } from "@/hooks/use-climax-run";
import { useRsRating } from "@/hooks/use-rs-rating";
import { useStageTransition } from "@/hooks/use-stage-transition";
import { useAtrVolatility } from "@/hooks/use-atr-volatility";
import { useSymbolSearch } from "@/hooks/use-symbol-search";
import { useStockQuote } from "@/hooks/use-stock-quote";
import { useTradingMode, getDefansifBlockMessage } from "@/hooks/use-trading-mode";
import { useTrades } from "@/hooks/use-trades";
import { useStockQuotes } from "@/hooks/use-stock-quote";

const SELECT = "h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus:outline-none focus:ring-1 focus:ring-ring";
const TEXTAREA = "w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm resize-none focus:outline-none focus:ring-1 focus:ring-ring";

interface InitialData {
  symbol?: string;
  strategy?: string;
  setup_type?: string;
  entry_date?: string;
  entry_price?: number;
}

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialData?: InitialData;
}

export function AddTradeDialog({ open, onOpenChange, initialData }: Props) {
  const addMutation = useAddTrade();
  const { data: setupTypes = [] } = useSetupTypes();

  const [symbol, setSymbol]         = useState("");
  const [strategy, setStrategy]     = useState("minervini");
  const [setupType, setSetupType]   = useState("vcp");
  // KARAR #477: Sinyal Kaynağı zorunlu (UX Bölüm 7) — initialData varsa
  // genelde Sinyaller sayfasından AL ile gelir → default "strategy"
  const [signalSource, setSignalSource] = useState<SignalSource>("strategy");
  const [entryDate, setEntryDate]   = useState("");
  const [entryPrice, setEntryPrice] = useState("");
  const [shares, setShares]         = useState("");
  const [status, setStatus]         = useState<TradeStatus>("open");
  // closed fields
  const [exitDate, setExitDate]     = useState("");
  const [exitPrice, setExitPrice]   = useState("");
  const [grade, setGrade]           = useState<TradeGrade>("B");
  const [exitReason, setExitReason] = useState<ExitReason>("stop_loss");
  const [lessons, setLessons]       = useState("");
  const [error, setError]           = useState<string | null>(null);
  // Paket 227 (27 May 2026): Vizyon İLKE #10 — Defansif modda submit BLOK
  // Mark canon TTLC s.187: "Defansif modda yeni AL pozisyonu yasak" disiplinini
  // explicit override checkbox ile koru. Sn. Ferit hatalı pozisyon açmasın.
  const [defansifOverride, setDefansifOverride] = useState(false);
  // KARAR ADAY #717 — Mark TTLC Sec 1 6 zorunlu plan alani (Mark birebir disiplin)
  const [planEntryTrigger, setPlanEntryTrigger]   = useState("");
  const [planStop, setPlanStop]                   = useState("");
  const [planTarget, setPlanTarget]               = useState("");
  const [planSizePct, setPlanSizePct]             = useState("");
  const [planExitStrategy, setPlanExitStrategy]   = useState("");
  const [planTimeHorizon, setPlanTimeHorizon]     = useState<TimeHorizon>("swing");
  // KARAR #720 alt (Paket 30): Mindset disiplin ön-kontrol
  const [mindsetReadToday, setMindsetReadToday] = useState(false);
  useEffect(() => {
    if (open) setMindsetReadToday(isReadTodayAny());
  }, [open]);

  // Paket 163 (26 May 2026): Symbol autocomplete (P149 Watchlist pateni)
  const [showSymbolSuggest, setShowSymbolSuggest] = useState(false);
  const { data: symbolSuggestions } = useSymbolSearch(symbol, 8);
  // Paket 163: yfinance canlı quote — entry_price önerisi
  const trimmedSym = symbol.trim().toUpperCase();
  const { data: quoteData } = useStockQuote(open && trimmedSym.length >= 2 ? trimmedSym : "");
  // Paket 196 (26 May 2026): Trading Mode kontrolü — Mark TTLC s.187 disiplini
  // Rehab → uyarı + %0.5 R önerisi, Defansif → BLOK, Agresif → Conviction High notu
  const tradingMode = useTradingMode();

  // Paket 202 (27 May 2026): Sektör konsantrasyon önizleme (Mark TTLC s.85).
  // Sn. Ferit yeni trade kayıt ederken sembol + miktar girince hesapla:
  // mevcut portfolio + bu trade → sektörün toplam % oranı. >30% kırmızı uyarı.
  const tradesQuery = useTrades();
  const openTradesAll = useMemo(
    () => (tradesQuery.data ?? []).filter((t) => t.status === "open"),
    [tradesQuery.data]
  );
  const openSymsAll = useMemo(
    () => Array.from(new Set(openTradesAll.map((t) => t.symbol))),
    [openTradesAll]
  );
  const openQuotesAll = useStockQuotes(openSymsAll);
  const sectorPreview = useMemo(() => {
    const newSector = symbolSuggestions?.find(
      (s) => s.symbol === trimmedSym
    )?.sector;
    const ep = parseFloat(entryPrice);
    const sh = parseInt(shares);
    if (!newSector || !ep || ep <= 0 || !sh || sh <= 0) return null;
    const newTradeValue = ep * sh;

    // Mevcut portfolio sektör değerleri (canlı fiyat × shares)
    const quoteMap = new Map<string, number>();
    openQuotesAll.forEach((r) => {
      if (r.data) quoteMap.set(r.data.symbol.toUpperCase(), r.data.price);
    });

    const sectorMap = new Map<string, number>();
    let totalCurrentValue = 0;
    openTradesAll.forEach((t) => {
      const price = quoteMap.get(t.symbol.toUpperCase()) ?? t.entry_price;
      const val = price * t.shares;
      totalCurrentValue += val;
      const sec = t.sector || "Bilinmiyor";
      sectorMap.set(sec, (sectorMap.get(sec) ?? 0) + val);
    });

    // Yeni trade ile + sektör projeksiyonu
    const newSectorTotal = (sectorMap.get(newSector) ?? 0) + newTradeValue;
    const newPortfolioTotal = totalCurrentValue + newTradeValue;
    const newSectorPct =
      newPortfolioTotal > 0 ? (newSectorTotal / newPortfolioTotal) * 100 : 0;

    return {
      sector: newSector,
      newSectorPct,
      warning: newSectorPct > 30,    // Mark TTLC s.85
      caution: newSectorPct > 20 && newSectorPct <= 30,
    };
  }, [symbolSuggestions, trimmedSym, entryPrice, shares, openTradesAll, openQuotesAll]);

  // KARAR #733 alt (Paket 33): Symbol Carr Stage pre-check (Stage 4 → uzak dur uyarısı)
  // Symbol ≥3 karakter olduğunda hook etkinleşir (gereksiz API gürültüsü engellenir)
  const trimmedSymbol = symbol.trim().toUpperCase();
  const { data: carrStage } = useCarrStage(
    open && trimmedSymbol.length >= 2 ? trimmedSymbol : undefined,
  );

  // KARAR #733 alt-paket (Paket 124, 26 May 2026): Mark Profili pre-check
  // Sembol girince Pivot+Climax+RS+Stage rozet bar göster — Mark canon checklist
  // trade açmadan önce. Sn. Ferit anında profili görür, disiplin pekişir.
  const markPreCheckSymbol = open && trimmedSymbol.length >= 2 ? trimmedSymbol : undefined;
  const { data: pivotData } = usePivotBreakout(markPreCheckSymbol);
  const { data: climaxData } = useClimaxRun(markPreCheckSymbol);
  const { data: rsData } = useRsRating(markPreCheckSymbol);
  const { data: stageData } = useStageTransition(markPreCheckSymbol);
  // KARAR #733 alt-paket (Paket 128, 26 May 2026): ATR Stop auto-fill
  // Mark TLSMW Ch 11: ATR-based stop "give the stock room to breathe"
  const { data: atrData } = useAtrVolatility(markPreCheckSymbol);

  // ATR Stop suggestion → planStop default (sadece kullanıcı henüz girmediyse)
  useEffect(() => {
    if (open && !planStop && atrData?.suggested_stop_normal != null) {
      setPlanStop(String(atrData.suggested_stop_normal));
    }
  }, [open, planStop, atrData?.suggested_stop_normal]);

  useEffect(() => {
    if (!open || !initialData) return;
    if (initialData.symbol !== undefined) setSymbol(initialData.symbol);
    if (initialData.strategy !== undefined) setStrategy(initialData.strategy);
    if (initialData.setup_type !== undefined) setSetupType(initialData.setup_type);
    if (initialData.entry_date !== undefined) setEntryDate(initialData.entry_date);
    if (initialData.entry_price !== undefined) setEntryPrice(String(initialData.entry_price));
    // KARAR #477: Sinyaller sayfasından AL ile geldiyse default "strategy" (sistem sinyali)
    setSignalSource("strategy");
  }, [open, initialData]);

  const isClosed = status === "closed";

  const plPreview = useMemo(() => {
    if (!isClosed || !entryPrice || !exitPrice || !shares) return null;
    const ep = parseFloat(entryPrice);
    const xp = parseFloat(exitPrice);
    const sh = parseInt(shares);
    if (isNaN(ep) || isNaN(xp) || isNaN(sh) || ep <= 0 || sh <= 0) return null;
    return calcPL(ep, xp, sh);
  }, [isClosed, entryPrice, exitPrice, shares]);

  function reset() {
    setSymbol(""); setStrategy("minervini"); setSetupType("vcp");
    setSignalSource("strategy");
    setEntryDate(""); setEntryPrice(""); setShares("");
    setStatus("open"); setExitDate(""); setExitPrice("");
    setGrade("B"); setExitReason("stop_loss"); setLessons(""); setError(null);
    // KARAR ADAY #717 plan alanlari reset
    setPlanEntryTrigger(""); setPlanStop(""); setPlanTarget("");
    setPlanSizePct(""); setPlanExitStrategy(""); setPlanTimeHorizon("swing");
    // Paket 227: Defansif override checkbox reset
    setDefansifOverride(false);
  }

  function handleOpenChange(v: boolean) {
    if (!v) reset();
    onOpenChange(v);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const sym = symbol.trim().toUpperCase();
    if (!sym) { setError("Hisse sembolü gerekli"); return; }
    if (!entryDate) { setError("Giriş tarihi gerekli"); return; }
    const ep = parseFloat(entryPrice);
    const sh = parseInt(shares);
    if (isNaN(ep) || ep <= 0) { setError("Geçerli giriş fiyatı gerekli"); return; }
    if (isNaN(sh) || sh <= 0) { setError("Geçerli adet gerekli"); return; }

    // KARAR ADAY #717 — Mark TTLC Sec 1 disiplini: 6 plan alani ZORUNLU
    const pStop   = parseFloat(planStop);
    const pTarget = parseFloat(planTarget);
    const pSize   = parseFloat(planSizePct);
    if (!planEntryTrigger.trim()) { setError("Plan: Giriş tetikleyicisi gerekli (Mark TTLC Sec 1)"); return; }
    if (isNaN(pStop)   || pStop   <= 0) { setError("Plan: Geçerli stop $ gerekli (Mark)"); return; }
    if (isNaN(pTarget) || pTarget <= 0) { setError("Plan: Geçerli hedef $ gerekli (Mark)"); return; }
    if (isNaN(pSize)   || pSize   <= 0 || pSize > 100) { setError("Plan: Pozisyon % (0-100 arası) gerekli"); return; }
    if (!planExitStrategy.trim()) { setError("Plan: Çıkış stratejisi gerekli (Mark)"); return; }

    // Paket 227 (27 May 2026): Vizyon İLKE #10 — Defansif modda override gerekli
    // Mark TTLC s.187: Defansif modda yeni AL pozisyonu YASAK. Sn. Ferit explicit
    // onay vermezse submit BLOK (görsel uyarıdan AKSİYON disiplinine evrim).
    // Sadece açık (open) trade'ler için — kapalı kayıt geçmiş için override gerekmez.
    if (!isClosed && tradingMode.mode === "defansif" && !defansifOverride) {
      // Paket 264 (P233 helper DRY): getDefansifBlockMessage tek kaynak mesaj
      const baseMsg = getDefansifBlockMessage(tradingMode.mode);
      setError(`${baseMsg} Aşağıdaki "Riski biliyorum" kutusunu işaretleyin.`);
      return;
    }

    const body: TradeCreate = {
      symbol: sym, strategy, setup_type: setupType,
      signal_source: signalSource,  // KARAR #477 zorunlu (UX Bölüm 7)
      entry_date: entryDate, entry_price: ep, shares: sh, status,
      // KARAR ADAY #717 — Mark TTLC Sec 1 6 alan
      plan_entry_trigger: planEntryTrigger.trim(),
      plan_stop: pStop,
      plan_target: pTarget,
      plan_size_pct: pSize,
      plan_exit_strategy: planExitStrategy.trim(),
      plan_time_horizon: planTimeHorizon,
    };
    if (isClosed) {
      if (!exitDate || !exitPrice) { setError("Kapalı trade için çıkış tarihi ve fiyatı gerekli"); return; }
      body.exit_date   = exitDate;
      body.exit_price  = parseFloat(exitPrice);
      body.grade       = grade;
      body.exit_reason = exitReason;
      body.lessons     = lessons.trim() || null;
    }
    setError(null);
    addMutation.mutate(body, {
      onSuccess: () => { reset(); onOpenChange(false); },
      onError: (err) => setError((err as Error).message),
    });
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-lg max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Yeni Trade</DialogTitle>
        </DialogHeader>

        {/* Paket 196 (26 May 2026): Trading Mode uyarısı (Vizyon İLKE #10).
            Defansif → BLOK kırmızı uyarı, Rehab → sarı %0.5 R, Agresif → mavi Conviction High notu.
            Normal mod: rozet yok (sade UX). */}
        {tradingMode.mode !== "normal" && (
          <div
            className="rounded-md border px-3 py-2 flex items-start gap-2 text-xs"
            style={{
              background: `color-mix(in srgb, ${tradingMode.color} 8%, transparent)`,
              borderColor: `${tradingMode.color}55`,
              color: tradingMode.color,
            }}
          >
            <span aria-hidden="true" className="text-base leading-none">
              {tradingMode.emoji}
            </span>
            <div className="flex-1">
              <span className="font-semibold">
                Trade Modu: {tradingMode.mode.toUpperCase()} (%
                {tradingMode.recommendedSizingPct === 0
                  ? "0 — yeni AL BLOK"
                  : tradingMode.recommendedSizingPct.toFixed(1).replace(".0", "") + " R sizing"}
                )
              </span>
              <span className="block mt-1 italic">{tradingMode.reason}</span>
              <span className="block mt-1 text-[10px] opacity-90">{tradingMode.uiBehavior}</span>
            </div>
          </div>
        )}

        {/* KARAR #733 alt (Paket 33): Stage 4 'UZAK DUR' Mark uyarısı */}
        {carrStage && carrStage.stage === 4 && (
          <div
            className="rounded-md border px-3 py-2 flex items-start gap-2 text-xs"
            style={{
              background: "rgba(220, 53, 69, 0.08)",
              borderColor: "rgba(220, 53, 69, 0.4)",
              color: "var(--mtp-danger)",
            }}
          >
            <AlertTriangle size={14} className="mt-0.5 shrink-0" />
            <div className="flex-1">
              <span className="font-semibold">
                ⛔ {trimmedSymbol} Carr Stage 4 (Declining)
              </span>
              <span className="block mt-1 italic">
                Mark felsefesi: <b>UZAK DUR</b>. 30W MA altı + slope negatif.
                Stage 2'ye dönene kadar yeni alım önerilmez (KARAR #733).
              </span>
            </div>
          </div>
        )}

        {/* Paket 239 (27 May 2026): PreTradeChecklist Mark canon 7 koşul pre-flight
            Sembol girince + plan alanları dolduruldukça canlı yenilenir.
            Disiplin: AddTradeDialog 5 katmanlı zinciri tamamlar (görme→submit→pre-flight). */}
        {markPreCheckSymbol && !isClosed && (
          <PreTradeChecklist
            symbol={trimmedSymbol}
            stage={carrStage?.stage}
            rsRating={rsData?.rs_rating ?? null}
            pivotPass={pivotData?.status === "CONFIRMED" || pivotData?.status === "WEAK"}
            planEntryTrigger={planEntryTrigger}
            planStop={parseFloat(planStop) || null}
            planTarget={parseFloat(planTarget) || null}
            entryPrice={parseFloat(entryPrice) || null}
            planSizePct={parseFloat(planSizePct) || null}
          />
        )}

        {/* KARAR #733 alt-paket (Paket 124, 26 May 2026): Mark Profili pre-check
            rozet bar — sembol girince Pivot+Climax+RS+Stage tek bakışta.
            Trade açmadan önce Mark canon checklist görür → disiplin pekişir. */}
        {markPreCheckSymbol && (pivotData || climaxData || rsData || stageData) && (
          <div
            className="rounded-md border px-3 py-2 flex flex-col gap-1.5 text-xs"
            style={{
              background: "rgba(75,156,211,0.05)",
              borderColor: "rgba(75,156,211,0.30)",
            }}
          >
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                Mark Profili
              </span>
              <span className="text-[10px] text-muted-foreground italic">
                ({trimmedSymbol} — trade öncesi checklist)
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-1.5">
              {/* Pivot rozet */}
              {pivotData?.status && (
                <span
                  className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                  style={{
                    background:
                      pivotData.status === "CONFIRMED"
                        ? "rgba(40,167,69,0.15)"
                        : pivotData.status === "WEAK"
                        ? "rgba(245,158,11,0.15)"
                        : pivotData.status === "NEAR_PIVOT"
                        ? "rgba(75,156,211,0.15)"
                        : "rgba(220,53,69,0.10)",
                    color:
                      pivotData.status === "CONFIRMED"
                        ? "var(--mtp-excellent)"
                        : pivotData.status === "WEAK"
                        ? "#F59E0B"
                        : pivotData.status === "NEAR_PIVOT"
                        ? "var(--mtp-good, #4B9CD3)"
                        : "var(--mtp-danger)",
                  }}
                  title={`Pivot — ${pivotData.mark_says}`}
                >
                  Pivot:{" "}
                  {pivotData.status === "CONFIRMED"
                    ? "AL ✓"
                    : pivotData.status === "WEAK"
                    ? "Zayıf ⚠️"
                    : pivotData.status === "NEAR_PIVOT"
                    ? "Yakın ⏳"
                    : "Altı ○"}
                </span>
              )}
              {/* RS rozet */}
              {rsData?.rs_rating != null && (
                <span
                  className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                  style={{
                    background:
                      rsData.rs_rating >= 80
                        ? "rgba(40,167,69,0.15)"
                        : rsData.rs_rating >= 70
                        ? "rgba(75,156,211,0.15)"
                        : rsData.rs_rating >= 50
                        ? "rgba(245,158,11,0.10)"
                        : "rgba(220,53,69,0.10)",
                    color:
                      rsData.rs_rating >= 80
                        ? "var(--mtp-excellent)"
                        : rsData.rs_rating >= 70
                        ? "var(--mtp-good, #4B9CD3)"
                        : rsData.rs_rating >= 50
                        ? "#F59E0B"
                        : "var(--mtp-danger)",
                  }}
                  title={`RS Rating — ${rsData.mark_says}`}
                >
                  RS {rsData.rs_rating}
                </span>
              )}
              {/* Climax rozet — sadece CLIMAX_TOP/POTENTIAL_CLIMAX uyarı */}
              {(climaxData?.category === "CLIMAX_TOP" ||
                climaxData?.category === "POTENTIAL_CLIMAX") && (
                <span
                  className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                  style={{
                    background:
                      climaxData.category === "CLIMAX_TOP"
                        ? "rgba(220,53,69,0.15)"
                        : "rgba(245,158,11,0.15)",
                    color:
                      climaxData.category === "CLIMAX_TOP"
                        ? "var(--mtp-danger)"
                        : "#F59E0B",
                  }}
                  title={`Climax — ${climaxData.mark_says}`}
                >
                  Climax {climaxData.category === "CLIMAX_TOP" ? "🔴" : "⚠️"}
                </span>
              )}
              {/* Stage rozet */}
              {stageData?.category && stageData.category !== "NO_TRANSITION" && (
                <span
                  className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                  style={{
                    background:
                      stageData.category === "CONFIRMED_STAGE_2"
                        ? "rgba(40,167,69,0.15)"
                        : stageData.category === "EARLY_STAGE_2"
                        ? "rgba(245,158,11,0.15)"
                        : "rgba(75,156,211,0.15)",
                    color:
                      stageData.category === "CONFIRMED_STAGE_2"
                        ? "var(--mtp-excellent)"
                        : stageData.category === "EARLY_STAGE_2"
                        ? "#F59E0B"
                        : "var(--mtp-good, #4B9CD3)",
                  }}
                  title={`Stage — ${stageData.mark_says}`}
                >
                  Stage:{" "}
                  {stageData.category === "CONFIRMED_STAGE_2"
                    ? "Onaylı ✓"
                    : stageData.category === "EARLY_STAGE_2"
                    ? "Erken ⚡"
                    : "Olgun ⏳"}
                </span>
              )}
            </div>
          </div>
        )}

        {/* KARAR #720 alt (Paket 30): Mindset disiplin ön-kontrol uyarı banner */}
        {!mindsetReadToday && (
          <div
            className="rounded-md border px-3 py-2 flex items-start gap-2 text-xs"
            style={{
              background: "rgba(245, 158, 11, 0.08)",
              borderColor: "rgba(245, 158, 11, 0.4)",
              color: "#F59E0B",
            }}
          >
            <Quote size={14} className="mt-0.5 shrink-0" />
            <div className="flex-1">
              <span className="font-semibold">Mark hatırlatması bugün okunmamış. </span>
              <span>Yeni trade öncesi zihinsel disiplin: </span>
              <Link
                href="/"
                onClick={() => onOpenChange(false)}
                className="underline font-semibold hover:no-underline"
              >
                Dashboard&apos;da oku →
              </Link>
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit} className="flex flex-col gap-3 py-2">
          {/* Row 1 — Symbol + Strategy */}
          {/* Paket 163 (26 May 2026): Symbol autocomplete (P149 Watchlist pateni) */}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5 relative">
              <Label htmlFor="at-sym">Hisse *</Label>
              <Input
                id="at-sym"
                value={symbol}
                onChange={(e) => {
                  setSymbol(e.target.value.toUpperCase());
                  setShowSymbolSuggest(true);
                }}
                onFocus={() => setShowSymbolSuggest(true)}
                onBlur={() => setTimeout(() => setShowSymbolSuggest(false), 150)}
                placeholder="NVDA, TSLA, AAPL..."
                maxLength={10}
                autoFocus
                autoComplete="off"
              />
              {showSymbolSuggest && symbolSuggestions && symbolSuggestions.length > 0 && (
                <ul
                  className="absolute z-50 top-full left-0 right-0 mt-1 max-h-56 overflow-y-auto rounded-md border bg-popover shadow-lg"
                  style={{ borderColor: "var(--border)" }}
                >
                  {symbolSuggestions.map((s) => (
                    <li
                      key={s.symbol}
                      role="option"
                      aria-selected={trimmedSym === s.symbol}
                      onMouseDown={(e) => {
                        e.preventDefault();
                        setSymbol(s.symbol);
                        setShowSymbolSuggest(false);
                      }}
                      className="px-3 py-2 text-sm cursor-pointer hover:bg-accent border-b last:border-b-0"
                      style={{ borderColor: "var(--border)" }}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-mono font-semibold flex items-center gap-1.5">
                          {s.symbol}
                          {/* Paket 211 (27 May 2026): yfinance fallback rozet */}
                          {s.source === "yfinance" && (
                            <span
                              className="text-[9px] font-bold px-1 py-0.5 rounded"
                              style={{
                                background: "color-mix(in srgb, var(--mtp-good, #4B9CD3) 18%, transparent)",
                                color: "var(--mtp-good, #4B9CD3)",
                              }}
                              title="Quanfina evren dışı — yfinance ile doğrulandı"
                            >
                              yf
                            </span>
                          )}
                        </span>
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                          {s.sector}
                        </span>
                      </div>
                      <div className="text-xs text-muted-foreground truncate mt-0.5">
                        {s.name}
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-strat">Strateji *</Label>
              <select id="at-strat" value={strategy} onChange={(e) => setStrategy(e.target.value)} className={SELECT}>
                <option value="minervini">Minervini</option>
                <option value="carr">Carr</option>
              </select>
            </div>
          </div>

          {/* Row 2 — Setup */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="at-setup">Setup *</Label>
            <select id="at-setup" value={setupType} onChange={(e) => setSetupType(e.target.value)} className={SELECT}>
              {setupTypes.length > 0
                ? setupTypes.map((s) => <option key={s.key} value={s.key}>{s.label}</option>)
                : <>
                    <option value="vcp">VCP</option>
                    <option value="pivot">Pivot</option>
                    <option value="pocket_pivot">Pocket Pivot</option>
                    <option value="power_play">Power Play</option>
                    <option value="cup_and_handle">Cup &amp; Handle</option>
                    <option value="flat_base">Flat Base</option>
                    <option value="pullback">Pullback</option>
                    <option value="coiled_spring">Coiled Spring</option>
                  </>
              }
            </select>
          </div>

          {/* Row 2.5 — Sinyal Kaynağı (KARAR #477, UX Bölüm 7 ZORUNLU) */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="at-source">
              Sinyal Kaynağı * <span className="text-xs text-muted-foreground font-normal">(disiplin için zorunlu)</span>
            </Label>
            <select
              id="at-source"
              value={signalSource}
              onChange={(e) => setSignalSource(e.target.value as SignalSource)}
              className={SELECT}
            >
              <option value="strategy">{SIGNAL_SOURCE_LABELS.strategy}</option>
              <option value="manual_self">{SIGNAL_SOURCE_LABELS.manual_self}</option>
              <option value="manual_external">{SIGNAL_SOURCE_LABELS.manual_external}</option>
            </select>
            <span className="text-xs text-muted-foreground">
              {SIGNAL_SOURCE_DESCRIPTIONS[signalSource]}
            </span>
          </div>

          {/* Row 3 — Entry Date + Entry Price + Shares */}
          <div className="grid grid-cols-3 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-edate">Giriş Tarihi *</Label>
              <Input id="at-edate" type="date" value={entryDate} onChange={(e) => setEntryDate(e.target.value)} />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-eprice" className="flex items-center justify-between">
                <span>Giriş $ *</span>
                {/* Paket 163: yfinance canlı fiyat öneri rozet (tek-tık doldur) */}
                {quoteData?.price != null && quoteData.source === "yfinance" && (
                  <button
                    type="button"
                    onClick={() => setEntryPrice(String(quoteData.price.toFixed(2)))}
                    className="text-[10px] font-semibold px-1.5 py-0.5 rounded hover:bg-accent transition-colors"
                    style={{
                      color: "var(--mtp-good, #4B9CD3)",
                      border: "1px solid var(--mtp-good, #4B9CD3)",
                    }}
                    title={`yfinance canlı: $${quoteData.price.toFixed(2)} — tek tık doldur`}
                  >
                    🔴 ${quoteData.price.toFixed(2)}
                  </button>
                )}
              </Label>
              <Input id="at-eprice" type="number" value={entryPrice} onChange={(e) => setEntryPrice(e.target.value)} placeholder="700.00" step="0.01" min="0" />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-shares">Adet *</Label>
              <Input id="at-shares" type="number" value={shares} onChange={(e) => setShares(e.target.value)} placeholder="50" min="1" />
            </div>
          </div>

          {/* Mark Risk Advisor — Sprint 4-bis.7 Faz 1 B paket (KARAR #914+#969+#970)
              KARAR #727 (24 May 2026): RBA gerçek veri bağı — strategy prop ile filtre */}
          <MarkRiskAdvisor entryPrice={entryPrice} shares={shares} strategy={strategy} />

          {/* Paket 202 (27 May 2026): Sektör konsantrasyon önizleme — Mark TTLC s.85
              Sn. Ferit yeni trade kayıt ederken sembol + entry + shares girince
              "Bu trade ile sektör %X olacak" — >30% kırmızı uyarı. */}
          {sectorPreview && (
            <div
              className="rounded-md border px-3 py-2 flex items-start gap-2 text-xs"
              style={{
                background: sectorPreview.warning
                  ? "color-mix(in srgb, var(--mtp-danger) 8%, transparent)"
                  : sectorPreview.caution
                  ? "color-mix(in srgb, #F59E0B 8%, transparent)"
                  : "color-mix(in srgb, var(--mtp-excellent) 6%, transparent)",
                borderColor: sectorPreview.warning
                  ? "var(--mtp-danger)"
                  : sectorPreview.caution
                  ? "#F59E0B"
                  : "var(--mtp-excellent)55",
                color: sectorPreview.warning
                  ? "var(--mtp-danger)"
                  : sectorPreview.caution
                  ? "#F59E0B"
                  : "inherit",
              }}
            >
              <span aria-hidden="true" className="text-base leading-none">
                {sectorPreview.warning ? "🚨" : sectorPreview.caution ? "⚠️" : "✓"}
              </span>
              <div className="flex-1">
                <span className="font-semibold">
                  Sektör projeksiyonu: {sectorPreview.sector}{" "}
                  <span className="font-mono tabular-nums ml-1">
                    {sectorPreview.newSectorPct.toFixed(0)}%
                  </span>
                </span>
                {sectorPreview.warning && (
                  <span className="block mt-1 italic">
                    Mark TTLC s.85 ihlali: max 25-30% tek sektör. Bu trade ile{" "}
                    {sectorPreview.newSectorPct.toFixed(0)}% — pozisyon küçült veya farklı sektörü değerlendir.
                  </span>
                )}
                {sectorPreview.caution && (
                  <span className="block mt-1 italic">
                    Sektör yoğunluğu artıyor (20-30%). Mark TTLC s.85 sınırına yaklaşıyorsun, plan büyütme dikkatli.
                  </span>
                )}
                {!sectorPreview.warning && !sectorPreview.caution && (
                  <span className="block mt-1 opacity-80">
                    Sektör dağılımı sağlıklı (≤20%). Mark TTLC s.85 ile uyumlu.
                  </span>
                )}
              </div>
            </div>
          )}

          {/* KARAR ADAY #732 (24 May 2026): Mark Pyramid Calculator (KARAR #487 3-Tier) */}
          <MarkPyramidCard entryPrice={entryPrice} shares={shares} />

          {/* KARAR ADAY #717 — Mark Trade Plan (TTLC Sec 1, 6 zorunlu alan) */}
          <div className="border rounded-md p-3 bg-muted/30 flex flex-col gap-3">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold flex items-center gap-2">
                <span>📋 Mark Trade Plan</span>
                <span className="text-xs font-normal text-muted-foreground">
                  (TTLC Sec 1 — "Without a written plan, you have only hope")
                </span>
              </h3>
              <span className="text-xs px-2 py-0.5 rounded bg-amber-100 dark:bg-amber-900/40 text-amber-900 dark:text-amber-200 font-medium">
                6 alan ZORUNLU
              </span>
            </div>

            {/* Plan Row A — Time Horizon + Size % */}
            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="at-plan-horizon">Süre Ufku *</Label>
                <select
                  id="at-plan-horizon"
                  value={planTimeHorizon}
                  onChange={(e) => setPlanTimeHorizon(e.target.value as TimeHorizon)}
                  className={SELECT}
                >
                  <option value="swing">{TIME_HORIZON_LABELS.swing}</option>
                  <option value="position">{TIME_HORIZON_LABELS.position}</option>
                  <option value="core">{TIME_HORIZON_LABELS.core}</option>
                </select>
                <span className="text-xs text-muted-foreground">
                  {TIME_HORIZON_DESCRIPTIONS[planTimeHorizon]}
                </span>
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="at-plan-size">Pozisyon % *</Label>
                <Input
                  id="at-plan-size"
                  type="number"
                  value={planSizePct}
                  onChange={(e) => setPlanSizePct(e.target.value)}
                  placeholder="6.25"
                  step="0.01"
                  min="0"
                  max="100"
                />
                <span className="text-xs text-muted-foreground">
                  Mark: Pilot %1-3 / Standart %6.25-12.5 / Full %15-25
                </span>
              </div>
            </div>

            {/* Paket 250 (27 May 2026): Mark Canon Otomatik Plan buton.
                Sn. Ferit entry_price'a sahipken tek tıkla Mark canon
                defaults: stop = entry × (1 - %6) = %6 stop (Mark TTLC s.131
                %7 mutlak limitin altında güvenli marj), target = entry +
                2×risk = 2R minimum (Mark canon). */}
            {entryPrice && parseFloat(entryPrice) > 0 && (
              <button
                type="button"
                onClick={() => {
                  const ep = parseFloat(entryPrice);
                  if (!ep || ep <= 0) return;
                  const markStop = (ep * 0.94).toFixed(2);   // %6 stop (TTLC s.131 %7 marjı içinde)
                  const risk = ep - parseFloat(markStop);
                  const markTarget = (ep + 2 * risk).toFixed(2);  // 2R minimum
                  setPlanStop(markStop);
                  setPlanTarget(markTarget);
                }}
                className="text-xs px-3 py-1.5 rounded-md border self-start font-medium hover:bg-accent transition-colors"
                style={{
                  background: "rgba(75,156,211,0.08)",
                  borderColor: "rgba(75,156,211,0.4)",
                  color: "var(--mtp-good, #4B9CD3)",
                }}
                title="Mark canon: stop = entry × 0.94 (%6 — TTLC s.131 %7 marjı), target = entry + 2R"
              >
                ⚡ Mark Otomatik Plan (%6 stop + 2R target)
              </button>
            )}

            {/* Paket 251 (27 May 2026): R/R Live Preview rozet — entry+stop+target
                değiştikçe anlık hesap. Mark canon TTLC Sec 4 R/R minimum 2:1.
                PreTradeChecklist'te de görünür ama bu rozet kullanıcının dikkatini
                doğrudan Plan alanlarının altında çeker. */}
            {(() => {
              const ep = parseFloat(entryPrice);
              const ps = parseFloat(planStop);
              const pt = parseFloat(planTarget);
              if (!ep || !ps || !pt || ep <= 0 || ps <= 0 || pt <= 0) return null;
              const risk = ep - ps;
              const reward = pt - ep;
              if (risk <= 0) return null;
              const rr = reward / risk;
              const stopPct = (risk / ep) * 100;
              const rrColor = rr >= 2 ? "var(--mtp-excellent)" : rr >= 1 ? "#F59E0B" : "var(--mtp-danger)";
              const stopColor = stopPct <= 7 ? "var(--mtp-excellent)" : "var(--mtp-danger)";
              return (
                <div
                  className="rounded-md border px-3 py-2 flex items-center gap-4 flex-wrap text-xs"
                  style={{
                    background: `color-mix(in srgb, ${rrColor} 6%, transparent)`,
                    borderColor: `${rrColor}55`,
                    fontFamily: "var(--font-jetbrains-mono, monospace)",
                  }}
                >
                  <span>
                    <b>R/R:</b>{" "}
                    <span style={{ color: rrColor, fontWeight: 700 }}>
                      {rr.toFixed(2)}:1
                    </span>{" "}
                    {rr >= 2 ? "✓ Mark uyumlu" : rr >= 1 ? "⚠️ Zayıf (Mark min 2:1)" : "🔴 Kabul edilemez"}
                  </span>
                  <span>
                    <b>Stop %:</b>{" "}
                    <span style={{ color: stopColor, fontWeight: 700 }}>
                      {stopPct.toFixed(1)}%
                    </span>{" "}
                    {stopPct <= 7 ? "✓" : "🔴 TTLC s.131 LİMİT AŞILDI"}
                  </span>
                  <span className="text-muted-foreground">
                    Risk: <b>${risk.toFixed(2)}</b> / Reward: <b>${reward.toFixed(2)}</b>
                  </span>
                </div>
              );
            })()}

            {/* Plan Row B — Stop + Target */}
            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="at-plan-stop">Stop $ *</Label>
                <Input
                  id="at-plan-stop"
                  type="number"
                  value={planStop}
                  onChange={(e) => setPlanStop(e.target.value)}
                  placeholder="entry'den önce yazılır"
                  step="0.01"
                  min="0"
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="at-plan-target">Hedef $ *</Label>
                <Input
                  id="at-plan-target"
                  type="number"
                  value={planTarget}
                  onChange={(e) => setPlanTarget(e.target.value)}
                  placeholder="2R-3R minimum"
                  step="0.01"
                  min="0"
                />
              </div>
            </div>

            {/* Plan Row C — Entry Trigger (textarea) */}
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-plan-trigger">Giriş Tetikleyicisi *</Label>
              <textarea
                id="at-plan-trigger"
                value={planEntryTrigger}
                onChange={(e) => setPlanEntryTrigger(e.target.value)}
                rows={2}
                placeholder="Mark: Neden bu trade? Örn. 'VCP 3 daralma + hacim teyitli pivot kırılımı 50DMA üstünde'"
                className={TEXTAREA}
              />
            </div>

            {/* Plan Row D — Exit Strategy (textarea) */}
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-plan-exit">Çıkış Stratejisi *</Label>
              <textarea
                id="at-plan-exit"
                value={planExitStrategy}
                onChange={(e) => setPlanExitStrategy(e.target.value)}
                rows={2}
                placeholder="Mark: Çıkış planı net. Örn. '2R'de yarı sat, kalanı 50MA trail. Outside Day → tam çıkış.'"
                className={TEXTAREA}
              />
            </div>
          </div>

          {/* Row 4 — Status */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="at-status">Statü</Label>
            <select id="at-status" value={status} onChange={(e) => setStatus(e.target.value as TradeStatus)} className={SELECT}>
              <option value="open">Açık</option>
              <option value="closed">Kapalı</option>
            </select>
          </div>

          {/* Closed fields */}
          {isClosed && (
            <>
              <div className="border-t pt-3 flex flex-col gap-3">
                <div className="grid grid-cols-2 gap-3">
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="at-xdate">Çıkış Tarihi *</Label>
                    <Input id="at-xdate" type="date" value={exitDate} onChange={(e) => setExitDate(e.target.value)} />
                  </div>
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="at-xprice">Çıkış $ *</Label>
                    <Input id="at-xprice" type="number" value={exitPrice} onChange={(e) => setExitPrice(e.target.value)} placeholder="826.00" step="0.01" min="0" />
                  </div>
                </div>

                {plPreview && (
                  <div className="text-sm rounded-md border px-3 py-2 flex gap-4" style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}>
                    <span style={{ color: plPreview.plDollar >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
                      {fmtPLDollar(plPreview.plDollar)}
                    </span>
                    <span style={{ color: plPreview.plPct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
                      {fmtPLPct(plPreview.plPct)}
                    </span>
                  </div>
                )}

                <div className="grid grid-cols-2 gap-3">
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="at-grade">Grade</Label>
                    <select id="at-grade" value={grade} onChange={(e) => setGrade(e.target.value as TradeGrade)} className={SELECT}>
                      {GRADE_OPTIONS.map((g) => <option key={g} value={g}>{g}</option>)}
                    </select>
                  </div>
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="at-reason">Çıkış Sebebi</Label>
                    <select id="at-reason" value={exitReason} onChange={(e) => setExitReason(e.target.value as ExitReason)} className={SELECT}>
                      {(Object.entries(EXIT_REASON_LABELS) as [ExitReason, string][]).map(([k, v]) => (
                        <option key={k} value={k}>{v}</option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="at-lessons">Dersler (opsiyonel)</Label>
                  <textarea id="at-lessons" value={lessons} onChange={(e) => setLessons(e.target.value)} rows={3} placeholder="Bu trade'den öğrendiklerim..." className={TEXTAREA} />
                </div>
              </div>
            </>
          )}

          {error && <p className="text-sm" style={{ color: "var(--mtp-danger)" }}>{error}</p>}

          {/* Paket 227 (27 May 2026): Vizyon İLKE #10 — Defansif modda override
              checkbox. Mark TTLC s.187 disiplini AKSİYON kilidi. Sadece açık trade
              + defansif modda görünür. Sn. Ferit "Riski biliyorum" işaretlemeden
              Kaydet butonu disabled. */}
          {!isClosed && tradingMode.mode === "defansif" && (
            <label
              className="flex items-start gap-2 rounded-md border px-3 py-2 text-xs cursor-pointer"
              style={{
                background: "rgba(220, 53, 69, 0.06)",
                borderColor: "rgba(220, 53, 69, 0.5)",
              }}
            >
              <input
                type="checkbox"
                checked={defansifOverride}
                onChange={(e) => setDefansifOverride(e.target.checked)}
                className="mt-0.5 shrink-0"
                style={{ accentColor: "var(--mtp-danger)" }}
              />
              <span style={{ color: "var(--mtp-danger)" }}>
                <b>Defansif modda yeni AL açıyorum.</b> Mark TTLC s.187 disiplinini
                aşıyorum — piyasa Stage 3-4 + MH&lt;30 olduğunu kabul ediyorum, riski
                kendim üstleniyorum.
              </span>
            </label>
          )}

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>İptal</Button>
            <Button
              type="submit"
              disabled={
                addMutation.isPending ||
                (!isClosed && tradingMode.mode === "defansif" && !defansifOverride)
              }
              title={
                !isClosed && tradingMode.mode === "defansif" && !defansifOverride
                  ? "Defansif modda yeni AL BLOK — 'Riski biliyorum' kutusunu işaretle"
                  : undefined
              }
            >
              {addMutation.isPending ? "Kaydediliyor..." : "Kaydet"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
