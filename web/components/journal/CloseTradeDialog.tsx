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
import { useUpdateTrade, useTrades } from "@/hooks/use-trades";
import { calcPL, fmtPLDollar, fmtPLPct } from "@/lib/math";
import type { Trade, TradeGrade, ExitReason, TradeUpdate } from "@/types/trade";
import { GRADE_OPTIONS, EXIT_REASON_LABELS } from "@/types/trade";
import { PlanVsRealityCard } from "./PlanVsRealityCard";
import { useTradingMode } from "@/hooks/use-trading-mode";

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

  // Paket 218 (27 May 2026): Mark canon kapanış analizi
  // R-Multiple = pl_dollar / risk_dollar (Mark TTLC Sec 4 — RBA temel metrik)
  // risk_dollar = (entry - plan_stop) × shares
  const rMultiple = useMemo(() => {
    if (!trade || !plPreview || !trade.plan_stop) return null;
    const riskDollar = (trade.entry_price - trade.plan_stop) * trade.shares;
    if (riskDollar <= 0) return null;
    return plPreview.plDollar / riskDollar;
  }, [trade, plPreview]);

  // Mevcut trading mod + bu kapanış sonrası mod öngörüsü
  // (Streak güncel veriyle Hook hesabı, kayıp/kazanç streak'i tahmin)
  const currentMode = useTradingMode();
  const tradesQuery = useTrades();
  const modePreview = useMemo(() => {
    if (!trade || !plPreview) return null;
    // Bu kayıp/kazanç ardışık streak'i etkilerse mod tahmini
    const willBeWin = plPreview.plDollar > 0;
    const willBeLoss = plPreview.plDollar < 0;
    const closedExcludingThis = (tradesQuery.data ?? [])
      .filter((t) => t.status === "closed" && t.id !== trade.id && t.pl_dollar != null)
      .sort((a, b) => (b.exit_date ?? "").localeCompare(a.exit_date ?? ""));

    // En yeni'den geriye streak (bu trade dahil olacak)
    let newWins = willBeWin ? 1 : 0;
    let newLosses = willBeLoss ? 1 : 0;
    let breakAtLoss = willBeWin;  // Win sonrası loss görürsek dur
    let breakAtWin = willBeLoss;  // Loss sonrası win görürsek dur

    for (const t of closedExcludingThis) {
      const pl = t.pl_dollar ?? 0;
      if (pl > 0) {
        if (breakAtWin) break;
        if (newLosses > 0) break;
        newWins += 1;
        breakAtLoss = true;
      } else if (pl < 0) {
        if (breakAtLoss) break;
        if (newWins > 0) break;
        newLosses += 1;
        breakAtWin = true;
      } else break;
    }

    let predictedMode: string = currentMode.mode;
    let modeShift = "";
    if (newLosses >= 3 && currentMode.mode !== "defansif") {
      predictedMode = "rehab";
      modeShift = currentMode.mode === "rehab"
        ? "Rehab modunda kalır (zincir devam)"
        : "→ Rehab moduna geçer (TTLC s.187 — %0.5 R)";
    } else if (newWins >= 5 && currentMode.mode === "agresif") {
      modeShift = "Agresif modda kalır (hot hand)";
    } else if (newWins >= 5) {
      predictedMode = "agresif (piyasa güçlüyse)";
      modeShift = "→ Agresif aday (piyasa MH>70 + 5+ kazanç)";
    } else if (currentMode.mode === "rehab" && newWins >= 1) {
      modeShift = "Rehab streak kırıldı (kazanç) — normal'a yaklaşılıyor";
    }

    return { predictedMode, modeShift, newWins, newLosses };
  }, [trade, plPreview, tradesQuery.data, currentMode]);

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
            <div className="text-sm rounded-md border px-3 py-2 flex items-center gap-4 flex-wrap" style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}>
              <span style={{ color: plPreview.plDollar >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
                {fmtPLDollar(plPreview.plDollar)}
              </span>
              <span style={{ color: plPreview.plPct >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
                {fmtPLPct(plPreview.plPct)}
              </span>
              {/* Paket 218: R-Multiple (Mark TTLC Sec 4 RBA temel metrik) */}
              {rMultiple != null && (
                <span
                  className="ml-auto text-xs px-2 py-0.5 rounded font-semibold"
                  style={{
                    background: rMultiple >= 1
                      ? "color-mix(in srgb, var(--mtp-excellent) 15%, transparent)"
                      : "color-mix(in srgb, var(--mtp-danger) 15%, transparent)",
                    color: rMultiple >= 1 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
                  }}
                  title="R-Multiple = pl_dollar / risk_dollar (Mark TTLC Sec 4)"
                >
                  {rMultiple >= 0 ? "+" : ""}{rMultiple.toFixed(2)}R
                </span>
              )}
            </div>
          )}

          {/* Paket 218: Mark Mod öngörü — bu kapanış sonrası mod tahmin */}
          {modePreview && modePreview.modeShift && (
            <div
              className="rounded-md border px-3 py-2 text-xs"
              style={{
                background: "color-mix(in srgb, var(--mtp-good, #4B9CD3) 6%, transparent)",
                borderColor: "color-mix(in srgb, var(--mtp-good, #4B9CD3) 40%, transparent)",
              }}
            >
              <div className="font-semibold mb-1 flex items-center gap-1.5">
                <span aria-hidden="true">{currentMode.emoji}</span>
                Mark Mod Öngörü
              </div>
              <div className="text-muted-foreground leading-relaxed">
                Mevcut: <b style={{ color: currentMode.color }}>{currentMode.mode.toUpperCase()}</b>{" "}
                ({currentMode.consecutiveWins > 0 ? `${currentMode.consecutiveWins} kazanç` : `${currentMode.consecutiveLosses} kayıp`} streak).
                <br />
                Bu kapanış sonrası streak:{" "}
                {modePreview.newWins > 0 ? `${modePreview.newWins} kazanç` : `${modePreview.newLosses} kayıp`}.
                <br />
                <span style={{ color: currentMode.color }}>{modePreview.modeShift}</span>
              </div>
            </div>
          )}

          {/* KARAR ADAY #725 (24 May 2026): Plan vs Reality karti (Mark TTLC Sec 4) */}
          {trade && <PlanVsRealityCard trade={trade} exitPrice={exitPrice} />}

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

          {/* P393: role="alert" a11y + data-testid Vitest hook (AddTradeDialog P392 pateni) */}
          {error && (
            <p
              role="alert"
              data-testid="close-trade-error"
              className="text-sm"
              style={{ color: "var(--mtp-danger)" }}
            >
              {error}
            </p>
          )}

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
