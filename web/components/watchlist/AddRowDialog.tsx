"use client";

import { useState } from "react";
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
import { useAddWatchlistRow } from "@/hooks/use-watchlist-mutations";
import type { WatchlistRowCreate, WatchlistStatus, WatchlistStrategy } from "@/types/watchlist";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const SELECT_CLASS =
  "h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus:outline-none focus:ring-1 focus:ring-ring";

export function AddRowDialog({ open, onOpenChange }: Props) {
  const addMutation = useAddWatchlistRow();

  const [symbol, setSymbol] = useState("");
  const [strategy, setStrategy] = useState<WatchlistStrategy>("minervini");
  const [status, setStatus] = useState<WatchlistStatus>("watch");
  const [setupType, setSetupType] = useState("");
  const [pivotPrice, setPivotPrice] = useState("");
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);

  function resetForm() {
    setSymbol("");
    setStrategy("minervini");
    setStatus("watch");
    setSetupType("");
    setPivotPrice("");
    setNote("");
    setError(null);
  }

  function handleOpenChange(v: boolean) {
    if (!v) resetForm();
    onOpenChange(v);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const sym = symbol.trim().toUpperCase();
    if (!sym) {
      setError("Hisse sembolü gerekli");
      return;
    }
    const body: WatchlistRowCreate = {
      symbol: sym,
      strategy,
      status,
      setup_type: setupType.trim() || null,
      pivot_price: pivotPrice ? parseFloat(pivotPrice) : null,
      note: note.trim() || null,
    };
    setError(null);
    addMutation.mutate(body, {
      onSuccess: () => { resetForm(); onOpenChange(false); },
      onError: (err) => setError((err as Error).message),
    });
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Hisse Ekle</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="flex flex-col gap-4 py-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ar-symbol">Hisse *</Label>
              <Input
                id="ar-symbol"
                value={symbol}
                onChange={(e) => setSymbol(e.target.value.toUpperCase())}
                placeholder="NVDA"
                maxLength={10}
                autoFocus
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ar-strategy">Strateji *</Label>
              <select
                id="ar-strategy"
                value={strategy}
                onChange={(e) => setStrategy(e.target.value as WatchlistStrategy)}
                className={SELECT_CLASS}
              >
                <option value="minervini">Minervini</option>
                <option value="carr">Carr</option>
              </select>
            </div>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="ar-status">Statü *</Label>
            <select
              id="ar-status"
              value={status}
              onChange={(e) => setStatus(e.target.value as WatchlistStatus)}
              className={SELECT_CLASS}
            >
              <option value="watch">Watch</option>
              <option value="on_deck">On Deck</option>
              <option value="focus">Focus</option>
              <option value="buy">Buy</option>
            </select>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ar-setup">Setup (opsiyonel)</Label>
              <Input
                id="ar-setup"
                value={setupType}
                onChange={(e) => setSetupType(e.target.value)}
                placeholder="VCP"
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ar-pivot">Pivot Fiyat (opsiyonel)</Label>
              <Input
                id="ar-pivot"
                type="number"
                value={pivotPrice}
                onChange={(e) => setPivotPrice(e.target.value)}
                placeholder="820.00"
                step="0.01"
                min="0"
              />
            </div>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="ar-note">Not (opsiyonel)</Label>
            <textarea
              id="ar-note"
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="MA50 destek..."
              rows={2}
              className="rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm resize-none focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>

          {error && (
            <p className="text-sm" style={{ color: "var(--mtp-danger)" }}>
              {error}
            </p>
          )}

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => handleOpenChange(false)}
            >
              İptal
            </Button>
            <Button type="submit" disabled={addMutation.isPending}>
              {addMutation.isPending ? "Ekleniyor..." : "Ekle"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
