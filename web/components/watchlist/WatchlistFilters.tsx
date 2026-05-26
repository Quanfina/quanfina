"use client";

import { STATUS_LABELS, STRATEGY_LABELS } from "@/types/watchlist";

interface WatchlistFiltersProps {
  strategy: string;
  onStrategyChange: (v: string) => void;
  status: string;
  onStatusChange: (v: string) => void;
  search: string;
  onSearchChange: (v: string) => void;
  // P114 (26 May 2026): LEADER only + Climax UYARI chip'leri
  leaderOnly: boolean;
  onLeaderOnlyChange: (v: boolean) => void;
  climaxOnly: boolean;
  onClimaxOnlyChange: (v: boolean) => void;
  // P129 (26 May 2026): Stage Onaylı chip (mark_signals.stage_category === "CONFIRMED_STAGE_2")
  stageConfirmedOnly: boolean;
  onStageConfirmedOnlyChange: (v: boolean) => void;
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
  leaderOnly, onLeaderOnlyChange,
  climaxOnly, onClimaxOnlyChange,
  stageConfirmedOnly, onStageConfirmedOnlyChange,
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

      {/* P114: LEADER only chip (rs_rating >= 80) */}
      <button
        onClick={() => onLeaderOnlyChange(!leaderOnly)}
        className={`text-xs font-semibold px-3 py-1.5 rounded border transition-colors ${
          leaderOnly
            ? "bg-[var(--mtp-excellent)] border-[var(--mtp-excellent)] text-white"
            : "border-input bg-background text-muted-foreground hover:border-[var(--mtp-excellent)] hover:text-[var(--mtp-excellent)]"
        }`}
        title="Sadece IBD LEADER hisseler (RS ≥ 80)"
      >
        L LEADER
      </button>

      {/* P114: Climax UYARI chip */}
      <button
        onClick={() => onClimaxOnlyChange(!climaxOnly)}
        className={`text-xs font-semibold px-3 py-1.5 rounded border transition-colors ${
          climaxOnly
            ? "bg-orange-500 border-orange-500 text-white"
            : "border-input bg-background text-muted-foreground hover:border-orange-500 hover:text-orange-500"
        }`}
        title="Sadece Climax Top uyarılı hisseler"
      >
        🔥 Climax
      </button>

      {/* P129 (26 May 2026): Stage Onaylı chip — Mark TLSMW Ch 4 CONFIRMED_STAGE_2 */}
      <button
        onClick={() => onStageConfirmedOnlyChange(!stageConfirmedOnly)}
        className={`text-xs font-semibold px-3 py-1.5 rounded border transition-colors ${
          stageConfirmedOnly
            ? "bg-purple-600 border-purple-600 text-white"
            : "border-input bg-background text-muted-foreground hover:border-purple-600 hover:text-purple-600"
        }`}
        title="Sadece Stage 2 Onaylı hisseler (Mark TLSMW Ch 4)"
      >
        ✓ Stage 2
      </button>

      <span className="text-xs font-mono text-muted-foreground ml-auto">
        {filteredRows}/{totalRows} satır
      </span>
    </div>
  );
}
