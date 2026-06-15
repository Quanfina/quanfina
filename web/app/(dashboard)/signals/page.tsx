"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useGridTheme } from "@/hooks/use-grid-theme";
import { useGridColumnState } from "@/hooks/use-grid-column-state";
import { Activity, Plus, X, RotateCcw } from "lucide-react";
import "@/lib/ag-grid-setup"; // AG Grid modül kaydı (Paket 355 — bundle split)
import { AgGridReact } from "ag-grid-react";
import type { ColDef, ICellRendererParams, ValueFormatterParams, CellClassParams } from "ag-grid-community";

// KARAR #479 (20 May 2026): DRY MONO style — Watchlist/Journal pateni.
// KARAR #484 (20 May 2026): lib/grid-styles ortak helper'a tasindi (Hisse Tarama
// ile birlikte DRY birlestirme — sayfa-ici kopyalama yasak).
import { MONO, MONO_RIGHT } from "@/lib/grid-styles";
import { useSignals } from "@/hooks/use-signals";
import { useStockQuotes } from "@/hooks/use-stock-quote";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { AddRowDialog } from "@/components/watchlist/AddRowDialog";
import { Button } from "@/components/ui/button";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import { formatDateTR, formatDayLabel, todayLocalISO } from "@/lib/format-date";
import { getPassedSignals, setPassedSignals, clearPassedSignals, signalKey } from "@/lib/passed-signals";
import { toast } from "sonner";
import type { Signal } from "@/types/signal";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { ModBadge } from "@/components/mark/ModBadge";
import { useTradingMode, isNewAlBlocked, getModUiTheme } from "@/hooks/use-trading-mode";
import { fmtUsd } from "@/lib/format-currency";
import { RsRatingBadge } from "@/components/shared/RsRatingBadge";
import { SignalRREnrichedCell } from "@/components/signals/SignalRREnrichedCell";

const SELECT =
  "h-8 rounded-md border border-input bg-background px-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring";

const STATUS_LABELS: Record<string, string> = {
  buy: "Buy",
  focus: "Focus",
  on_deck: "On Deck",
  watch: "Watch",
};

const STATUS_COLORS: Record<string, string> = {
  buy: "#28A745",
  focus: "#4B9CD3",
  on_deck: "#F59E0B",
  watch: "var(--muted-foreground)",
};

const STRATEGY_LABELS: Record<string, string> = {
  minervini: "Minervini",
  carr: "Carr",
};

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3 flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-lg font-semibold">{value}</span>
    </div>
  );
}

// KARAR #470 (20 May 2026): Sinyaller AG Grid tablo (KARAR #423 + #469 revize).
// Sn. Ferit: "kart şeklinde değil tablo şeklinde olsun". Watchlist/Journal pateni.
// Konsensus mantığı KARAR #469 ile zaten kaldırıldı — her watchlist satırı 1 sinyal satırı.
export default function SignalsPage() {
  const { gridClass } = useGridTheme();
  const { data, isLoading, isError, error, refetch, isFetching } = useSignals();
  const gridRef = useRef<AgGridReact<Signal>>(null);
  const gridColumnState = useGridColumnState("quanfina-signals-cols");
  // Paket 235 (27 May 2026): Defansif modda AL butonu disabled (Mark TTLC s.187)
  const tradingMode = useTradingMode();
  const alBlocked = isNewAlBlocked(tradingMode.mode);

  const [statusFilter, setStatusFilter] = useState<"all" | "buy" | "focus_buy">("all");
  const [strategyFilter, setStrategyFilter] = useState<"all" | "minervini" | "carr">("all");
  const [newTodayOnly, setNewTodayOnly] = useState(false);
  const [showPassed, setShowPassed] = useState(false);
  // P130 (26 May 2026): Climax Uyarı filter chip — Mark TLSMW Ch 9 SAT sinyali
  const [climaxWarnOnly, setClimaxWarnOnly] = useState(false);

  const [tradeOpen, setTradeOpen] = useState(false);
  const [tradeSignal, setTradeSignal] = useState<Signal | null>(null);
  // KARAR #478 (UX Bölüm 5): Manuel sinyal ekleme — Watchlist'e satır ekler,
  // Sinyaller listesi otomatik yenilenir (consensus_count + status hesaplanır).
  const [manualOpen, setManualOpen] = useState(false);

  // KARAR #475 (20 May 2026): localStorage-backed passed signals (UX Bölüm 6 "AL/GEÇ").
  // Initial load: SSR hydration uyumu için useEffect (window guard).
  const [passedKeys, setPassedKeys] = useState<Set<string>>(new Set());
  useEffect(() => {
    setPassedKeys(getPassedSignals());
  }, []);

  // KARAR #476: gridClass useGridTheme'den (SSR uyumu)

  function handleTradeClick(signal: Signal) {
    setTradeSignal(signal);
    setTradeOpen(true);
  }

  function handlePassClick(signal: Signal) {
    const key = signalKey(signal.symbol, signal.strategy);
    const next = new Set(passedKeys);
    next.add(key);
    setPassedKeys(next);
    setPassedSignals(next);
    toast.success(`${signal.symbol} (${signal.strategy}) geçildi`, {
      action: {
        label: "Geri Al",
        onClick: () => {
          const undo = new Set(next);
          undo.delete(key);
          setPassedKeys(undo);
          setPassedSignals(undo);
        },
      },
      duration: 4000,
    });
  }

  // Stable callback refs (Watchlist pateni)
  const tradeClickRef = useRef<(s: Signal) => void>(handleTradeClick);
  tradeClickRef.current = handleTradeClick;
  const passClickRef = useRef<(s: Signal) => void>(handlePassClick);
  passClickRef.current = handlePassClick;

  const columnDefs = useMemo<ColDef<Signal>[]>(() => [
    {
      field: "symbol",
      headerName: "Sembol",
      width: 110,
      pinned: "left" as const,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const s = p.data;
        if (!s) return null;
        return (
          <div className="flex items-center gap-2 h-full">
            {/* P433 (31 May 2026 — BUG FIX): sembol artık /hisse detay sayfasına
                tıklanabilir Link (İzleme Listesi SymbolCellRenderer pateni). Önce
                düz <span> idi — Sn. Ferit "hisseye tıklayınca grafiğe gidemiyorum". */}
            <Link
              href={`/hisse/${s.symbol}`}
              className="font-semibold tracking-tight hover:underline hover:text-foreground transition-colors"
              style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            >
              {s.symbol}
            </Link>
            {s.is_new_today && (
              <span
                className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full"
                style={{ background: "#28A745", color: "#fff" }}
                title="Bugün eklendi"
              >
                YENİ
              </span>
            )}
          </div>
        );
      },
    },
    {
      field: "strategy",
      headerName: "Strateji",
      width: 110,
      valueFormatter: (p) => STRATEGY_LABELS[p.value as string] ?? (p.value as string),
    },
    {
      field: "status",
      headerName: "Statü",
      width: 100,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const s = p.value as string;
        return (
          <span
            className="text-xs font-semibold"
            style={{ color: STATUS_COLORS[s] ?? "inherit" }}
          >
            {STATUS_LABELS[s] ?? s}
          </span>
        );
      },
    },
    {
      field: "setup_type",
      headerName: "Setup",
      width: 160,
      valueFormatter: (p) => (p.value as string | null) ?? "—",
    },
    // KARAR #733 alt-paket (Paket 99, 26 May 2026): RS Rating kategori rozet
    // (Watchlist P98 paten birebir — Mark TLSMW Ch 3-5 / IBD canon visual).
    // Paket 158 (26 May 2026): document.createElement -> RsRatingBadge React component
    // (React 19 crash önleme — DRY shared/RsRatingBadge.tsx).
    {
      field: "rs_rating",
      headerName: "RS",
      width: 100,
      minWidth: 85,
      cellRenderer: RsRatingBadge,
    },
    {
      field: "price",
      headerName: "Fiyat",
      // P438 (Kural #28): canlı yfinance fiyatı (current_price enrich), bayat
      // web_watchlist.price snapshot fallback.
      headerTooltip: "Canlı yfinance fiyatı (yfinance erişilemezse snapshot)",
      width: 100,
      type: "rightAligned",
      valueGetter: (p) => p.data?.current_price ?? p.data?.price,
      valueFormatter: (p) => fmtUsd(p.value as number),
      // P445 (review Kural #28 fix): quote_source tüketilir (eski dead field).
      // Mock (yfinance erişilemiyor) → amber + tooltip "sentetik"; eski hali
      // mock fiyatı sessizce "Canlı yfinance" gibi gösteriyordu.
      cellStyle: (p: CellClassParams<Signal>) =>
        p.data?.quote_source === "mock"
          ? { ...MONO_RIGHT, color: "#F59E0B" }
          : MONO_RIGHT,
      tooltipValueGetter: (p) =>
        p.data?.quote_source === "mock"
          ? "Sentetik fiyat — yfinance erişilemiyor (paper trading için güvenilmez)"
          : "Canlı yfinance fiyatı",
    },
    {
      field: "stop_loss",
      headerName: "Stop",
      // P468 (15 Haz 2026): headerTooltip — Stop son GÜNLÜK KAPANIŞ bazlı (2.5×ATR),
      // canlı Fiyat ile birebir örtüşmeyebilir (review bulgusu: baz netliği).
      headerTooltip:
        "Son günlük kapanış bazlı (2.5×ATR, Mark Risk-first). Canlı Fiyat ile birebir örtüşmeyebilir.",
      width: 90,
      type: "rightAligned",
      valueFormatter: (p) => fmtUsd(p.value as number | null),
      cellStyle: { ...MONO_RIGHT, color: "var(--mtp-danger)" },
    },
    {
      field: "target_price",
      headerName: "Hedef",
      headerTooltip:
        "Son günlük kapanış + 3R projeksiyon. Canlı Fiyat ile birebir örtüşmeyebilir (sabit grafik hedefi değil).",
      width: 90,
      type: "rightAligned",
      valueFormatter: (p) => fmtUsd(p.value as number | null),
      cellStyle: { ...MONO_RIGHT, color: "var(--mtp-excellent)" },
    },
    {
      field: "risk_reward",
      // P113 (26 May 2026): R/R + RS rozet + Climax ikon birlesik hucre.
      // KARAR #473: R/R = (hedef - fiyat) / (fiyat - stop). Backend hesaplar.
      // Paket 158 (26 May 2026): DOM createElement -> SignalRREnrichedCell React component
      // (React 19 crash önleme — components/signals/SignalRREnrichedCell.tsx).
      // P468 (15 Haz 2026): headerTooltip — P466 hedef = giris + 3R (ATR stop bazli)
      // oldugu icin R/R sabit ~3.0 gorunur. Tooltip "projeksiyon, grafik hedefi degil"
      // netligi verir (komsu Fiyat/Bagil Hacim kolonlari ayni headerTooltip patern).
      headerName: "R/R",
      headerTooltip:
        "R/R = (hedef − fiyat) / (fiyat − stop). Hedef = giriş + 3R projeksiyon " +
        "(2.5×ATR stop bazlı, Mark R-multiple) — sabit grafik hedefi değil, bu yüzden ~3.0 sabit.",
      width: 150,
      type: "rightAligned",
      cellRenderer: SignalRREnrichedCell,
    },
    {
      field: "added_date",
      headerName: "Eklenme",
      // P417 (31 May 2026): formatDayLabel DRY (P416 Watchlist paten — Sn. Ferit
      // Focus List patenli "Pzt/Salı..." hızlı okuma). lib/format-date'de tek kaynak.
      // Eski formatDateTR DD.MM.YYYY HH:MM detaylıydı — değer kaybı yok, son 6 gün
      // gün adıyla görünür, 7+ gün eski tarih formatı (eskiyle uyumlu).
      width: 110,
      valueFormatter: (p) => formatDayLabel(p.value as string | null),
      cellStyle: MONO,
      headerTooltip: "Eklenme — son 6 gün gün adı, eski tarihler DD.MM.YYYY",
    },
    // P417 (31 May 2026): Bağıl Hacim sütunu — Mark TLSMW Bol. 6 "Hacim teyit"
    // canon. Pivot kırılım kararı için hacim doğrulaması (≥1.5 patlama / <0.7
    // sönük). P416 Watchlist paten DRY paralel.
    {
      headerName: "Bağıl Hcm",
      field: "relative_volume",
      width: 100,
      type: "rightAligned",
      valueFormatter: (p: ValueFormatterParams<Signal, number>) =>
        p.value == null ? "—" : `${p.value.toFixed(2)}×`,
      cellStyle: (p: CellClassParams<Signal, number>) => {
        const v = p.value;
        let color = "var(--muted-foreground)";
        if (v != null) {
          if (v >= 1.5) color = "var(--mtp-excellent)";
          else if (v < 0.7) color = "var(--mtp-danger)";
          else if (v >= 1.0) color = "var(--mtp-neutral)";
        }
        return { ...MONO, color, fontWeight: v != null && v >= 1.5 ? 600 : 400, textAlign: "right" };
      },
      headerTooltip: "Bağıl Hacim: bugün / 50-gün ortalama (Mark TLSMW Bol. 6).",
    },
    // KARAR #726 (24 May 2026): Mark Profili kolonu (DRY MarkBadgeStrip 4. sayfa)
    {
      headerName: "Mark Profili",
      minWidth: 220,
      flex: 1,
      sortable: false,
      filter: false,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const sig = p.data;
        if (!sig?.mark_signals) return <span className="text-xs text-muted-foreground">—</span>;
        return <MarkBadgeStrip signals={sig.mark_signals} density="compact" showEmpty={false} />;
      },
    },
    // KARAR #733 alt-paket (Paket 81): Pivot kolonu — Mark TLSMW Ch 10
    // AL/Zayıf/Yakın/Altı kompakt rozet (DRY MarkBadgeStrip pateni paralel).
    {
      headerName: "Pivot",
      field: "pivot_status",
      width: 110,
      sortable: false,
      filter: false,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const status = p.data?.pivot_status;
        if (!status) return <span className="text-xs text-muted-foreground">—</span>;
        const meta =
          status === "CONFIRMED"
            ? { label: "AL ✓", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.15)" }
            : status === "WEAK"
            ? { label: "Zayıf ⚠️", color: "#F59E0B", bg: "rgba(245,158,11,0.15)" }
            : status === "NEAR_PIVOT"
            ? { label: "Yakın ⏳", color: "var(--mtp-neutral)", bg: "rgba(75,156,211,0.15)" }
            : { label: "Altı ○", color: "var(--mtp-danger)", bg: "rgba(220,53,69,0.10)" };
        return (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
            style={{
              background: meta.bg,
              color: meta.color,
              border:
                status === "CONFIRMED"
                  ? "1px solid var(--mtp-excellent)"
                  : "1px solid currentColor",
            }}
            title={`Mark TLSMW Ch 10 — Pivot ${status}`}
          >
            {meta.label}
          </span>
        );
      },
    },
    // KARAR #733 alt-paket (Paket 123, 26 May 2026): Stage Transition kolonu
    // Mark TLSMW Ch 4 + Weinstein — Stage 1→2 geçiş kategorisi (Pivot pateni).
    {
      headerName: "Stage",
      field: "mark_signals.stage_category" as keyof Signal,
      width: 110,
      sortable: false,
      filter: false,
      valueGetter: (p) => p.data?.mark_signals?.stage_category ?? null,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const cat = p.data?.mark_signals?.stage_category;
        if (!cat) return <span className="text-xs text-muted-foreground">—</span>;
        const meta =
          cat === "CONFIRMED_STAGE_2"
            ? { label: "Onaylı ✓", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.15)" }
            : cat === "EARLY_STAGE_2"
            ? { label: "Erken ⚡", color: "#F59E0B", bg: "rgba(245,158,11,0.15)" }
            : cat === "STAGE_2_MATURE"
            ? { label: "Olgun ⏳", color: "var(--mtp-neutral)", bg: "rgba(75,156,211,0.15)" }
            : { label: "Yok ○", color: "var(--muted-foreground)", bg: "rgba(128,128,128,0.10)" };
        return (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
            style={{
              background: meta.bg,
              color: meta.color,
              border:
                cat === "CONFIRMED_STAGE_2"
                  ? "1px solid var(--mtp-excellent)"
                  : "1px solid currentColor",
            }}
            title={`Mark TLSMW Ch 4 — Stage ${cat}`}
          >
            {meta.label}
          </span>
        );
      },
    },
    {
      headerName: "",
      width: 130,
      pinned: "right" as const,
      sortable: false,
      resizable: false,
      suppressMovable: true,
      // KARAR #475 (UX Bölüm 6): AL + GEÇ ikili buton — mekanik karar
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const signal = p.data;
        if (!signal) return null;
        return (
          <div className="flex items-center justify-center gap-1 h-full">
            {/* Paket 235: Defansif modda AL disabled (Mark TTLC s.187) */}
            <button
              type="button"
              onClick={() => !alBlocked && tradeClickRef.current(signal)}
              disabled={alBlocked}
              className="inline-flex items-center gap-1 h-6 rounded-md px-2 text-[11px] font-semibold transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
              style={{ background: alBlocked ? "#9CA3AF" : "#28A745", color: "#fff" }}
              title={
                alBlocked
                  ? "Defansif mod aktif — Mark TTLC s.187: yeni AL BLOK (AddTradeDialog override mevcut)"
                  : "Trade aç (form pre-fill ile)"
              }
            >
              <Plus size={11} />
              AL
            </button>
            <button
              type="button"
              onClick={() => passClickRef.current(signal)}
              className="inline-flex items-center gap-1 h-6 rounded-md border px-2 text-[11px] font-medium hover:bg-accent transition-colors text-muted-foreground"
              title="Bu sinyali geç (gizle)"
            >
              <X size={11} />
              GEÇ
            </button>
          </div>
        );
      },
    },
  ], [alBlocked]); // Paket 235: alBlocked değişince cellRenderer yenilensin

  const defaultColDef = useMemo<ColDef<Signal>>(() => ({
    sortable: true,
    resizable: true,
    filter: false,
  }), []);

  // P438 (Kural #28): canlı fiyat enrich (Watchlist paten). Signal.price =
  // web_watchlist.price snapshot (insert anında set, UPDATE yok) → bayat
  // (NVDA $875 bölünme öncesi senaryosu). useStockQuotes ile canlı yfinance.
  const allSymbols = useMemo(
    () => Array.from(new Set((data ?? []).map((s) => s.symbol))),
    [data]
  );
  const quoteResults = useStockQuotes(allSymbols);
  const quoteMap = useMemo(() => {
    const map = new Map<string, { price: number; source: string }>();
    quoteResults.forEach((r) => {
      if (r.data) map.set(r.data.symbol.toUpperCase(), { price: r.data.price, source: r.data.source });
    });
    return map;
  }, [quoteResults]);

  const filtered = useMemo(() => {
    let rows = data ?? [];
    // KARAR #475 — Geçilen sinyaller default gizli (showPassed true ise göster)
    if (!showPassed) {
      rows = rows.filter((s) => !passedKeys.has(signalKey(s.symbol, s.strategy)));
    }
    if (statusFilter === "buy") rows = rows.filter((s) => s.status === "buy");
    if (statusFilter === "focus_buy")
      rows = rows.filter((s) => s.status === "buy" || s.status === "focus");
    if (strategyFilter !== "all") rows = rows.filter((s) => s.strategy === strategyFilter);
    if (newTodayOnly) rows = rows.filter((s) => s.is_new_today);
    // P130: Climax Uyarı filter — CLIMAX_TOP veya POTENTIAL_CLIMAX
    if (climaxWarnOnly) {
      rows = rows.filter(
        (s) =>
          s.mark_signals?.climax_category === "CLIMAX_TOP" ||
          s.mark_signals?.climax_category === "POTENTIAL_CLIMAX"
      );
    }
    // P438: canlı quote enrich — Fiyat kolonu current_price gösterir (bayat fallback)
    return rows.map((s) => {
      const q = quoteMap.get(s.symbol.toUpperCase());
      return q ? { ...s, current_price: q.price, quote_source: q.source as "yfinance" | "mock" } : s;
    });
  }, [data, statusFilter, strategyFilter, newTodayOnly, passedKeys, showPassed, climaxWarnOnly, quoteMap]);

  const totalSignals = data?.length ?? 0;
  const newTodayCount = data?.filter((s) => s.is_new_today).length ?? 0;
  const passedCount = passedKeys.size;
  const strongest = data?.[0];
  // KARAR #733 alt-paket (Paket 36): Stage 4 sayisi banner icin
  const stage4Count = data?.filter((s) => s.mark_signals?.carr_stage === 4).length ?? 0;

  const tradeInitial = tradeSignal
    ? {
        symbol: tradeSignal.symbol,
        strategy: tradeSignal.strategy,
        setup_type: tradeSignal.setup_type ?? undefined,
        entry_date: todayLocalISO(), // Paket 356: yerel tarih (UTC off-by-one fix)
        entry_price: tradeSignal.price,
      }
    : undefined;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Sinyaller</h1>
          <p className="text-sm text-muted-foreground">
            Tüm strateji sinyalleri — bugün ne var?
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Paket 215 (27 May 2026): ModBadge compact — Sn. Ferit AL butonu basmadan
              mod farkındalığı. Defansif modda yeni AL BLOK, Rehab'da %0.5 R uyarısı. */}
          <ModBadge variant="compact" />
          <Button size="sm" onClick={() => setManualOpen(true)} title="Manuel sinyal ekle (İzleme Listesi üzerinden)">
            <Plus size={14} className="mr-1.5" />
            Manuel Sinyal
          </Button>
        </div>
      </div>

      {/* Paket 262 (27 May 2026): Sinyaller Defansif uyarı banner — Mark TTLC s.187.
          getModUiTheme DRY helper (P259) kullanır. P260+P261 paten devam. */}
      {(() => {
        const theme = getModUiTheme(tradingMode.mode);
        if (!theme || tradingMode.mode !== "defansif") return null;
        return (
          <div
            className="px-6 py-2 border-b text-xs flex items-center gap-2"
            style={{
              background: theme.background,
              borderColor: theme.borderColor,
              color: theme.color,
            }}
          >
            <span aria-hidden="true">{theme.emoji}</span>
            <span>
              <b>{theme.shortMessage}</b> AL sinyalleri görsel inceleme amaçlı — AL
              butonu disabled (gri), AddTradeDialog override gerektirir.
            </span>
          </div>
        );
      })()}

      {/* KARAR #733 alt-paket (Paket 36): Mark Regime üst-uyarı banner */}
      <MarkRegimeBanner
        stage4Count={stage4Count}
        totalCount={totalSignals}
        climaxTopCount={(data ?? []).filter((s) => s.mark_signals?.climax_category === "CLIMAX_TOP").length}
      />

      {/* Stats */}
      {!isLoading && !isError && (
        <div className="px-6 py-3 border-b grid grid-cols-3 gap-3">
          <StatCard label="Yeni Bugün" value={newTodayCount > 0 ? `${newTodayCount} sinyal` : "—"} />
          <StatCard label="Toplam Sinyal" value={`${totalSignals} sinyal`} />
          <StatCard
            label="En Güçlü"
            value={
              strongest
                ? `${strongest.symbol} (RS ${Math.round(strongest.rs_rating)})`
                : "—"
            }
          />
        </div>
      )}

      {/* Filters */}
      <div className="px-6 py-2 border-b flex flex-wrap items-center gap-2">
        <select
          value={statusFilter}
          onChange={(e) =>
            setStatusFilter(e.target.value as "all" | "buy" | "focus_buy")
          }
          className={SELECT}
        >
          <option value="all">Statü: Tümü</option>
          <option value="buy">Sadece Buy</option>
          <option value="focus_buy">Buy ve Focus</option>
        </select>

        <select
          value={strategyFilter}
          onChange={(e) =>
            setStrategyFilter(e.target.value as "all" | "minervini" | "carr")
          }
          className={SELECT}
        >
          <option value="all">Strateji: Tümü</option>
          <option value="minervini">Minervini</option>
          <option value="carr">Carr</option>
        </select>

        <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
          <input
            type="checkbox"
            checked={newTodayOnly}
            onChange={(e) => setNewTodayOnly(e.target.checked)}
            className="h-3.5 w-3.5 accent-primary"
          />
          Yeni Bugün
        </label>

        {/* P130 (26 May 2026): Climax UYARI filter chip — Mark TLSMW Ch 9 SAT sinyali */}
        <button
          onClick={() => setClimaxWarnOnly(!climaxWarnOnly)}
          className={`text-xs font-semibold px-3 py-1.5 rounded border transition-colors ${
            climaxWarnOnly
              ? "bg-[var(--mtp-danger)] border-[var(--mtp-danger)] text-white"
              : "border-input bg-background text-muted-foreground hover:border-[var(--mtp-danger)] hover:text-[var(--mtp-danger)]"
          }`}
          title="Sadece Climax UYARI sinyalleri (CLIMAX_TOP / POTENTIAL_CLIMAX) — Mark TLSMW Ch 9"
        >
          🔴 Climax UYARI
        </button>

        {passedCount > 0 && (
          <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
            <input
              type="checkbox"
              checked={showPassed}
              onChange={(e) => setShowPassed(e.target.checked)}
              className="h-3.5 w-3.5 accent-primary"
            />
            Geçilenleri göster ({passedCount})
          </label>
        )}

        {passedCount > 0 && (
          <button
            type="button"
            onClick={() => {
              setPassedKeys(new Set());
              setPassedSignals(new Set());
              toast.success("Tüm geçilen sinyaller geri alındı");
            }}
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Tüm geçilen sinyalleri geri al"
          >
            <RotateCcw size={11} />
            Geçişleri sıfırla
          </button>
        )}

        <span className="text-xs text-muted-foreground ml-1">
          {filtered.length} / {totalSignals} sinyal
        </span>
      </div>

      {/* Content */}
      <div className="flex-1 px-6 py-4">
        {isLoading && (
          <div className={`${gridClass} h-[600px] w-full`}>
            <GridLoadingOverlay />
          </div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-64 px-6">
            <div
              className="max-w-xl w-full p-4 border rounded-md"
              style={{ borderColor: "var(--mtp-danger)", background: "rgba(255, 80, 80, 0.06)" }}
              role="alert"
            >
              <div className="font-semibold mb-1" style={{ color: "var(--mtp-danger)" }}>
                ⚠️ Sinyal verisi alınamadı
              </div>
              <div className="text-xs mb-3 opacity-80">
                {(error as Error)?.message ?? "Bilinmeyen hata"}
              </div>
              <div className="text-xs mb-3 opacity-70">
                Olası sebep: Cloud SQL erişilemez (instance durmuş veya IP whitelist eski).
                GCP Console → SQL → instance durum kontrol et.
              </div>
              <Button size="sm" variant="outline" onClick={() => refetch()} disabled={isFetching}>
                {isFetching ? "Tekrar deneniyor..." : "Tekrar Dene"}
              </Button>
            </div>
          </div>
        )}
        {!isLoading && !isError && filtered.length === 0 && (
          <div
            className="flex flex-col items-center justify-center h-64 gap-3 text-center text-muted-foreground"
            data-testid="signals-empty-state"
          >
            <Activity size={32} strokeWidth={1.5} />
            {/* P411: Akıllı empty state — backend doluyken UI boş gözüküyor mu?
                passedKeys.size > 0 ve showPassed kapalı ise net açıklama + reset. */}
            {totalSignals > 0 && passedKeys.size > 0 && !showPassed ? (
              <>
                <p className="text-sm" style={{ color: "var(--foreground)" }}>
                  {passedKeys.size} sinyal &ldquo;geçilmiş&rdquo; olarak işaretli ve gizli
                </p>
                <p className="text-xs">
                  Bunlar Sn. Ferit&apos;in &ldquo;GEÇ&rdquo; butonuna bastığı sinyaller (localStorage,
                  KARAR #475). Aşağıdan seçin:
                </p>
                <div className="flex items-center gap-2 mt-1">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setShowPassed(true)}
                    data-testid="signals-empty-show-passed"
                  >
                    Geçilenleri Göster
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      if (window.confirm(`${passedKeys.size} geçilmiş sinyal sıfırlanacak. Emin misin?`)) {
                        setPassedKeys(new Set());
                        clearPassedSignals();
                        toast.success(`${passedKeys.size} geçilmiş sinyal sıfırlandı`);
                      }
                    }}
                    data-testid="signals-empty-reset-passed"
                    style={{ color: "var(--mtp-danger)" }}
                    title="Yıkıcı eylem (Kural #4) — confirm dialog"
                  >
                    Sıfırla ({passedKeys.size})
                  </Button>
                </div>
              </>
            ) : (
              <>
                <p className="text-sm">Bu filtreyle sinyal yok.</p>
                <p className="text-xs">
                  Filtre seçimini değiştirin veya İzleme Listesi üzerinden yeni sinyal ekleyin.
                </p>
              </>
            )}
          </div>
        )}
        {!isLoading && !isError && filtered.length > 0 && (
          <div className={gridClass} style={{ height: 600, width: "100%" }}>
            <AgGridReact<Signal>
              ref={gridRef}
              {...gridColumnState}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={defaultColDef}
              rowData={filtered}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
              getRowId={(p) => `${p.data.symbol}-${p.data.strategy}`}
            />
          </div>
        )}
      </div>

      <AddTradeDialog
        open={tradeOpen}
        onOpenChange={(v) => {
          setTradeOpen(v);
          if (!v) setTradeSignal(null);
        }}
        initialData={tradeInitial}
      />

      {/* KARAR #478: Manuel Sinyal Ekleme (UX Bölüm 5) — AddRowDialog yeniden kullanımı */}
      <AddRowDialog open={manualOpen} onOpenChange={setManualOpen} />
    </div>
  );
}
