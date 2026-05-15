"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { useUpdateWatchlistRow } from "@/hooks/use-watchlist-mutations";
import type { WatchlistRow } from "@/types/watchlist";

interface Props {
  row: WatchlistRow | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function EditNoteDialog({ row, open, onOpenChange }: Props) {
  const updateMutation = useUpdateWatchlistRow();
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (row) setNote(row.note ?? "");
  }, [row]);

  function handleOpenChange(v: boolean) {
    if (!v) setError(null);
    onOpenChange(v);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!row) return;
    setError(null);
    updateMutation.mutate(
      {
        symbol: row.symbol,
        strategy: row.strategy,
        update: { note: note.trim() || null },
      },
      {
        onSuccess: () => onOpenChange(false),
        onError: (err) => setError((err as Error).message),
      }
    );
  }

  const strategyLabel = row?.strategy === "minervini" ? "Minervini" : "Carr";

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>
            Not Düzenle — {row?.symbol}{" "}
            <span className="text-muted-foreground font-normal text-sm">
              ({strategyLabel})
            </span>
          </DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="flex flex-col gap-4 py-2">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="en-note">Not</Label>
            <textarea
              id="en-note"
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="Notunuzu buraya yazın..."
              rows={3}
              className="rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm resize-none focus:outline-none focus:ring-1 focus:ring-ring"
              autoFocus
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
            <Button type="submit" disabled={updateMutation.isPending}>
              {updateMutation.isPending ? "Kaydediliyor..." : "Kaydet"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
