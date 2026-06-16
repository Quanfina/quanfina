"use client";

import { useEffect, useState } from "react";
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
import type { Trade, TradeUpdate } from "@/types/trade";

const TEXTAREA =
  "w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm resize-none focus:outline-none focus:ring-1 focus:ring-ring";

interface Props {
  trade: Trade | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/**
 * Migration 011 / KARAR ADAY #960 (16 Haz 2026): Açık pozisyon AKTİF stop/hedef düzenleme.
 *
 * Mark "audible" disiplini (TTLC) — stop/hedef pozisyon açıldıktan sonra ayarlanabilir AMA
 * SEBEP zorunlu (sebepsiz kaydırma = disiplinsizlik). plan_stop/plan_target = orijinal plan
 * (referans, değişmez); stop_loss/target_price = aktif değerler. Backend (#960) sebep yoksa
 * 422 döner — UI de aynı disiplini önden uygular.
 */
export function EditOpenTradeDialog({ trade, open, onOpenChange }: Props) {
  const updateMutation = useUpdateTrade();
  const [stop, setStop] = useState("");
  const [target, setTarget] = useState("");
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (trade) {
      const s = trade.stop_loss ?? trade.plan_stop;
      const t = trade.target_price ?? trade.plan_target;
      setStop(s != null ? String(s) : "");
      setTarget(t != null ? String(t) : "");
      setReason("");
      setError(null);
    }
  }, [trade]);

  const curStop = trade?.stop_loss ?? trade?.plan_stop ?? null;
  const curTarget = trade?.target_price ?? trade?.plan_target ?? null;
  const sNum = parseFloat(stop);
  const tNum = parseFloat(target);
  const stopChanged = !isNaN(sNum) && sNum !== curStop;
  const targetChanged = !isNaN(tNum) && tNum !== curTarget;
  const changed = stopChanged || targetChanged;

  function handleOpenChange(v: boolean) {
    if (!v) setError(null);
    onOpenChange(v);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!trade) return;
    if (!changed) {
      setError("Stop veya hedefte değişiklik yok.");
      return;
    }
    if (isNaN(sNum) || sNum <= 0 || isNaN(tNum) || tNum <= 0) {
      setError("Geçerli aktif stop ve hedef (> 0) gerekli.");
      return;
    }
    if (!reason.trim()) {
      setError("Stop/hedef değişikliği için sebep zorunlu — Mark Audible disiplini (#960).");
      return;
    }
    setError(null);
    const update: TradeUpdate = {
      stop_loss: sNum,
      target_price: tNum,
      audible_reason: reason.trim(),
    };
    updateMutation.mutate(
      { id: trade.id, update },
      {
        onSuccess: () => onOpenChange(false),
        onError: (err) => setError((err as Error).message),
      }
    );
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Stop / Hedef Düzenle — {trade?.symbol}</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="flex flex-col gap-3 py-2">
          {trade && (trade.plan_stop != null || trade.plan_target != null) && (
            <div
              className="text-xs rounded-md border px-3 py-2 text-muted-foreground"
              style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            >
              Plan (değişmez): stop {trade.plan_stop != null ? `$${trade.plan_stop}` : "—"} · hedef{" "}
              {trade.plan_target != null ? `$${trade.plan_target}` : "—"}
            </div>
          )}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="et-stop">Aktif Stop *</Label>
              <Input id="et-stop" type="number" value={stop} onChange={(e) => setStop(e.target.value)} step="0.01" min="0" autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="et-target">Aktif Hedef *</Label>
              <Input id="et-target" type="number" value={target} onChange={(e) => setTarget(e.target.value)} step="0.01" min="0" />
            </div>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="et-reason">Sebep {changed ? "*" : "(değişiklik yoksa gerekmez)"}</Label>
            <textarea
              id="et-reason"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              rows={2}
              placeholder="Örn: trailing stop breakeven üstüne, kâr realizasyonu kademesi..."
              className={TEXTAREA}
            />
            <p className="text-[11px] text-muted-foreground">
              Mark &quot;audible&quot; disiplini — stop/hedef sebepsiz kaydırılmaz (#960).
            </p>
          </div>
          {error && (
            <p role="alert" data-testid="edit-trade-error" className="text-sm" style={{ color: "var(--mtp-danger)" }}>
              {error}
            </p>
          )}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>
              İptal
            </Button>
            <Button type="submit" disabled={updateMutation.isPending}>
              {updateMutation.isPending ? "Kaydediliyor..." : "Güncelle"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
