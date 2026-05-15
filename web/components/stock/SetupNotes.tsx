import type { WatchlistRow } from "@/types/watchlist";

const STRATEGY_LABELS: Record<string, string> = {
  minervini: "Minervini",
  carr: "Carr",
};

export function SetupNotes({ strategies }: { strategies: WatchlistRow[] }) {
  const noted = strategies.filter((s) => s.note);
  if (!noted.length) return null;

  return (
    <div className="rounded-lg border px-4 py-3 flex flex-col gap-2">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
        Notlar
      </h3>
      {noted.map((s, i) => (
        <div key={i} className="text-sm flex gap-2">
          <span className="font-medium text-muted-foreground shrink-0 w-24">
            {STRATEGY_LABELS[s.strategy] ?? s.strategy}:
          </span>
          <span>{s.note}</span>
        </div>
      ))}
    </div>
  );
}
