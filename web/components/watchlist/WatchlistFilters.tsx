"use client";

import { STATUS_LABELS, STRATEGY_LABELS } from "@/types/watchlist";

interface WatchlistFiltersProps {
  strategy: string;
  onStrategyChange: (v: string) => void;
  status: string;
  onStatusChange: (v: string) => void;
  search: string;
  onSearchChange: (v: string) => void;
  totalRows: number;
  filteredRows: number;
}

const SELECT_CLS =
  "text-sm rounded border border-input bg-background px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-ring";

// KARAR ADAY (21 May 2026): Konsensus kavrami kaldirildi. Her strateji ayri satir
// kanon. Sn. Ferit talimat: "konsesus kalksin nasil olsa her strateji tabloda
// farkli satirda gorukucek belki stop seviyeleri farkli olacak". Trader gercegi:
// Minervini ve Carr ayri stop/hedef/R-R hesaplar, konsensus tek satir gorunum bunu
// siler. minConsensus prop + filter UI cikarildi.
export function WatchlistFilters({
  strategy, onStrategyChange,
  status, onStatusChange,
  search, onSearchChange,
  totalRows, filteredRows,
}: WatchlistFiltersProps) {
  return (
    <div className="flex items-center gap-3 flex-wrap">
      <label className="text-sm font-medium text-muted-foreground">Strateji:</label>
      <select
        value={strategy}
        onChange={(e) => onStrategyChange(e.target.value)}
        className={SELECT_CLS}
      >
        {(Object.keys(STRATEGY_LABELS) as (keyof typeof STRATEGY_LABELS)[]).map((k) => (
          <option key={k} value={k}>{STRATEGY_LABELS[k]}</option>
        ))}
      </select>

      <label className="text-sm font-medium text-muted-foreground">Statü:</label>
      <select
        value={status}
        onChange={(e) => onStatusChange(e.target.value)}
        className={SELECT_CLS}
      >
        {(Object.keys(STATUS_LABELS) as (keyof typeof STATUS_LABELS)[]).map((k) => (
          <option key={k} value={k}>{STATUS_LABELS[k]}</option>
        ))}
      </select>

      <input
        type="text"
        value={search}
        onChange={(e) => onSearchChange(e.target.value)}
        placeholder="Hisse ara..."
        className={SELECT_CLS}
      />

      <span className="text-xs font-mono text-muted-foreground ml-auto">
        {filteredRows}/{totalRows} satır
      </span>
    </div>
  );
}
