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
import type { TradeCreate, TradeGrade, ExitReason, TradeStatus } from "@/types/trade";
import { GRADE_OPTIONS, EXIT_REASON_LABELS } from "@/types/trade";

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

  useEffect(() => {
    if (!open || !initialData) return;
    if (initialData.symbol !== undefined) setSymbol(initialData.symbol);
    if (initialData.strategy !== undefined) setStrategy(initialData.strategy);
    if (initialData.setup_type !== undefined) setSetupType(initialData.setup_type);
    if (initialData.entry_date !== undefined) setEntryDate(initialData.entry_date);
    if (initialData.entry_price !== undefined) setEntryPrice(String(initialData.entry_price));
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
    setEntryDate(""); setEntryPrice(""); setShares("");
    setStatus("open"); setExitDate(""); setExitPrice("");
    setGrade("B"); setExitReason("stop_loss"); setLessons(""); setError(null);
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

    const body: TradeCreate = {
      symbol: sym, strategy, setup_type: setupType,
      entry_date: entryDate, entry_price: ep, shares: sh, status,
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
