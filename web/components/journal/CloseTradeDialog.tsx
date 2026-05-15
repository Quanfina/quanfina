"use client";

import { useEffect, useMemo, useState } from "react";
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
import { useUpdateTrade } from "@/hooks/use-trades";
import { calcPL, fmtPLDollar, fmtPLPct } from "@/lib/math";
import type { Trade, TradeGrade, ExitReason, TradeUpdate } from "@/types/trade";
import { GRADE_OPTIONS, EXIT_REASON_LABELS } from "@/types/trade";

const SELECT = "h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus:outline-none focus:ring-1 focus:ring-ring";
const TEXTAREA = "w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm resize-none focus:outline-none focus:ring-1 focus:ring-ring";

interface Props {
  trade: Trade | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CloseTradeDialog({ trade, open, onOpenChange }: Props) {
  const updateMutation = useUpdateTrade();

  const [exitDate, setExitDate]     = useState("");
  const [exitPrice, setExitPrice]   = useState("");
  const [grade, setGrade]           = useState<TradeGrade>("B");
  const [exitReason, setExitReason] = useState<ExitReason>("stop_loss");
  const [lessons, setLessons]       = useState("");
  const [error, setError]           = useState<string | null>(null);

  useEffect(() => {
    if (trade) {
      setExitDate(trade.exit_date ?? "");
      setExitPrice(trade.exit_price != null ? String(trade.exit_price) : "");
      setGrade((trade.grade as TradeGrade) ?? "B");
      setExitReason((trade.exit_reason as ExitReason) ?? "stop_loss");
      setLessons(trade.lessons ?? "");
      setError(null);
    }
  }, [trade]);

  const plPreview = useMemo(() => {
    if (!trade || !exitPrice) return null;
    const xp = parseFloat(exitPrice);
    if (isNaN(xp)) return null;
    return calcPL(trade.entry_price, xp, trade.shares);
  }, [trade, exitPrice]);

  function handleOpenChange(v: boolean) {
    if (!v) setError(null);
    onOpenChange(v);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!trade) return;
    if (!exitDate || !exitPrice) { setError("Çıkış tarihi ve fiyatı gerekli"); return; }
    const xp = parseFloat(exitPrice);
    if (isNaN(xp) || xp <= 0) { setError("Geçerli çıkış fiyatı gerekli"); return; }
    setError(null);
    const update: TradeUpdate = {
      exit_date: exitDate,
      exit_price: xp,
      status: "closed",
      grade,
      exit_reason: exitReason,
      lessons: lessons.trim() || null,
    };
    updateMutation.mutate(
      { id: trade.id, update },
      {
        onSuccess: () => onOpenChange(false),
        onError: (err) => setError((err as Error).message),
      }
    );
  }

  const isEdit = trade?.status === "closed";
  const title = isEdit
    ? `Düzenle — ${trade?.symbol}`
    : `Kapat — ${trade?.symbol}`;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="flex flex-col gap-3 py-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ct-xdate">Çıkış Tarihi *</Label>
              <Input id="ct-xdate" type="date" value={exitDate} onChange={(e) => setExitDate(e.target.value)} autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ct-xprice">Çıkış Fiyatı *</Label>
              <Input id="ct-xprice" type="number" value={exitPrice} onChange={(e) => setExitPrice(e.target.value)} placeholder="826.00" step="0.01" min="0" />
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
              <Label htmlFor="ct-grade">Grade</Label>
              <select id="ct-grade" value={grade} onChange={(e) => setGrade(e.target.value as TradeGrade)} className={SELECT}>
                {GRADE_OPTIONS.map((g) => <option key={g} value={g}>{g}</option>)}
              </select>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ct-reason">Çıkış Sebebi</Label>
              <select id="ct-reason" value={exitReason} onChange={(e) => setExitReason(e.target.value as ExitReason)} className={SELECT}>
                {(Object.entries(EXIT_REASON_LABELS) as [ExitReason, string][]).map(([k, v]) => (
                  <option key={k} value={k}>{v}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="ct-lessons">Dersler (opsiyonel)</Label>
            <textarea id="ct-lessons" value={lessons} onChange={(e) => setLessons(e.target.value)} rows={3} placeholder="Bu trade'den öğrendiklerim..." className={TEXTAREA} />
          </div>

          {error && <p className="text-sm" style={{ color: "var(--mtp-danger)" }}>{error}</p>}

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>İptal</Button>
            <Button type="submit" disabled={updateMutation.isPending}>
              {updateMutation.isPending ? "Kaydediliyor..." : isEdit ? "Güncelle" : "Kapat"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
