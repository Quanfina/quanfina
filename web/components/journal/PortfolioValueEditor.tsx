"use client";

import { useState } from "react";
import { Pencil, Check, X } from "lucide-react";
import { usePortfolioValue } from "@/hooks/use-portfolio-value";
import { fmtUsd } from "@/lib/format-currency";

/**
 * P400: Portföy büyüklüğü inline editor.
 *
 * Sn. Ferit'in gerçek portföyünü girip persist edebilmesi için küçük UI.
 * AddTradeDialog + RiskYönetimi sayfasında kullanılabilir.
 *
 * Mark "Objektif Ayna Dil" (İLKE #11): sayı + birim, motivasyon yok.
 * a11y: edit modda input autoFocus + ESC/Enter kısayollar.
 */
export function PortfolioValueEditor() {
  const { value, setValue, defaultValue } = usePortfolioValue();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(String(value));

  function startEdit() {
    setDraft(String(value));
    setEditing(true);
  }

  function commit() {
    const n = parseFloat(draft);
    if (Number.isFinite(n) && n > 0) {
      setValue(n);
    }
    setEditing(false);
  }

  function cancel() {
    setEditing(false);
    setDraft(String(value));
  }

  const isDefault = value === defaultValue;

  if (editing) {
    return (
      <div className="flex items-center gap-1.5 text-xs">
        <span className="text-muted-foreground">Portföy:</span>
        <span className="text-muted-foreground">$</span>
        <input
          type="number"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") commit();
            if (e.key === "Escape") cancel();
          }}
          min={1}
          step={100}
          autoFocus
          aria-label="Portföy büyüklüğü ($)"
          className="w-24 rounded-md border border-input bg-background px-2 py-0.5 text-xs tabular-nums focus:outline-none focus:ring-1 focus:ring-ring"
        />
        <button
          type="button"
          onClick={commit}
          aria-label="Kaydet"
          className="rounded-md p-1 hover:bg-accent text-[color:var(--mtp-excellent)]"
        >
          <Check size={12} />
        </button>
        <button
          type="button"
          onClick={cancel}
          aria-label="İptal"
          className="rounded-md p-1 hover:bg-accent text-muted-foreground"
        >
          <X size={12} />
        </button>
      </div>
    );
  }

  return (
    <button
      type="button"
      onClick={startEdit}
      data-testid="portfolio-value-editor"
      className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors group"
      aria-label="Portföy büyüklüğünü düzenle"
    >
      <span>
        Portföy: <span className="tabular-nums font-medium text-foreground">{fmtUsd(value)}</span>
        {isDefault && <span className="ml-1 opacity-60">(varsayılan)</span>}
      </span>
      <Pencil size={11} className="opacity-50 group-hover:opacity-100" />
    </button>
  );
}
