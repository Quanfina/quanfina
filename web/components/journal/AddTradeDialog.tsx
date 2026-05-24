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
import { isReadTodayAny } from "@/lib/mindset-read-state";
import Link from "next/link";
import { Quote, AlertTriangle } from "lucide-react";
import { useCarrStage } from "@/hooks/use-carr-stage";

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

  // KARAR #733 alt (Paket 33): Symbol Carr Stage pre-check (Stage 4 → uzak dur uyarısı)
  // Symbol ≥3 karakter olduğunda hook etkinleşir (gereksiz API gürültüsü engellenir)
  const trimmedSymbol = symbol.trim().toUpperCase();
  const { data: carrStage } = useCarrStage(
    open && trimmedSymbol.length >= 2 ? trimmedSymbol : undefined,
  );

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
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="at-sym">Hisse *</Label>
              <Input id="at-sym" value={symbol} onChange={(e) => setSymbol(e.target.value.toUpperCase())} placeholder="NVDA" maxLength={10} autoFocus />
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
              <Label htmlFor="at-eprice">Giriş $ *</Label>
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

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>İptal</Button>
            <Button type="submit" disabled={addMutation.isPending}>
              {addMutation.isPending ? "Kaydediliyor..." : "Kaydet"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
