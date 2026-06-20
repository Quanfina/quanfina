import { suggestStatus, statusRank } from "@/lib/watchlist-status";

const STATUS_CONFIG: Record<string, { label: string; bg: string; color: string }> = {
  buy:    { label: "Buy",     bg: "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)", color: "var(--mtp-excellent)" },
  focus:  { label: "Focus",   bg: "color-mix(in srgb, var(--mtp-good)      18%, transparent)", color: "var(--mtp-good)"      },
  on_deck:{ label: "On Deck", bg: "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)", color: "var(--mtp-neutral)"   },
  watch:  { label: "Watch",   bg: "transparent",                                                color: "inherit"              },
};

// P562 (List Degradation): AG Grid params .data ile satırın pivot/pocket sinyalleri gelir.
// Doğrudan <StatusBadge value=.../> çağrıları (WatchlistSplitView, ActiveStrategies) data
// vermez → nudge gösterilmez (graceful). Sadece watchlist tablosunda terfi/düşüş önerisi.
interface StatusBadgeData {
  status?: string;
  pivot_status?: string | null;
  pocket_pivot?: string | null;
}

export function StatusBadge({ value, data }: { value?: string; data?: StatusBadgeData }) {
  const current = value ?? data?.status ?? "";
  const c = STATUS_CONFIG[current] ?? { label: current || "—", bg: "transparent", color: "inherit" };

  // Önerilen tier (pivot yakınlığı — Mark canon). Mevcut ile fark varsa nudge.
  let nudge: { dir: "up" | "down"; label: string; color: string; tip: string } | null = null;
  if (data && (data.pivot_status || data.pocket_pivot)) {
    const suggested = suggestStatus(data.pivot_status, data.pocket_pivot);
    const cr = statusRank(current);
    const sr = statusRank(suggested);
    if (cr !== -1 && sr !== -1 && sr !== cr) {
      const up = sr > cr;
      nudge = {
        dir: up ? "up" : "down",
        label: STATUS_CONFIG[suggested]?.label ?? suggested,
        color: up ? "var(--mtp-excellent)" : "#92400E",
        tip: up
          ? `Pivot ısınıyor → ${STATUS_CONFIG[suggested]?.label}'a terfi önerisi (Mark TLSMW Ch 10 pivot yakınlığı). Manuel terfi et.`
          : `Pivot zayıfladı → ${STATUS_CONFIG[suggested]?.label}'a düşüş önerisi (Mark Ch 10). Pozisyonu gözden geçir.`,
      };
    }
  }

  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          padding: "2px 8px",
          borderRadius: 4,
          background: c.bg,
          color: c.color,
          fontSize: 12,
          fontWeight: 500,
        }}
      >
        {c.label}
      </span>
      {nudge && (
        <span
          title={nudge.tip}
          data-testid="status-suggestion"
          style={{
            display: "inline-flex",
            alignItems: "center",
            fontSize: 10,
            fontWeight: 700,
            color: nudge.color,
            cursor: "help",
          }}
        >
          {nudge.dir === "up" ? "↑" : "↓"}
          {nudge.label}
        </span>
      )}
    </span>
  );
}
