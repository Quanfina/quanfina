"use client";

import { STATUS_LABELS, STRATEGY_LABELS } from "@/types/watchlist";

interface WatchlistFiltersProps {
  strategy: string;
  onStrategyChange: (v: string) => void;
  status: string;
  onStatusChange: (v: string) => void;
  minConsensus: number;
  onMinConsensusChange: (v: number) => void;
  search: string;
  onSearchChange: (v: string) => void;
  totalRows: number;
  filteredRows: number;
}

const SELECT_CLS =
  "text-sm rounded border border-input bg-background px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-ring";

export function WatchlistFilters({
  strategy, onStrategyChange,
  status, onStatusChange,
  minConsensus, onMinConsensusChange,
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

      <label className="text-sm font-medium text-muted-foreground">Konsensus:</label>
      <select
        value={minConsensus}
        onChange={(e) => onMinConsensusChange(Number(e.target.value))}
        className={SELECT_CLS}
      >
        <option value={0}>Tümü</option>
        <option value={1}>1+</option>
        <option value={2}>2+</option>
        <option value={3}>3+</option>
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
