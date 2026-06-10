"use client";

/**
 * Paket 158 (26 May 2026): RS Rating kategori rozet — React component.
 *
 * Mark TLSMW Ch 3-5 / IBD canon:
 * - >=80 → LEADER (yeşil L)
 * - >=70 → STRONG (mavi S)
 * - >=50 → AVERAGE (amber A)
 * - <50  → LAGGARD (kırmızı ↓ — Mark: uzak dur)
 *
 * DRY: Signals/Screens/Watchlist sayfalarında ortak (React 19 crash önleme).
 * Önceki kanon document.createElement DOM döndürüyordu — React 19 strict mode
 * "Objects are not valid as a React child" crash neden oluyordu (P153 + P158).
 */
export interface RsRatingBadgeProps {
  value?: number | null;
}

export function RsRatingBadge(p: RsRatingBadgeProps) {
  const rs = p.value;
  if (rs == null) {
    return <span style={{ color: "var(--muted-foreground)" }}>—</span>;
  }
  const rsRounded = Math.round(rs);
  let meta: { label: string; color: string; tip: string };
  if (rsRounded >= 80) meta = { label: "L", color: "var(--mtp-excellent)", tip: "IBD LEADER (Top 20%)" };
  else if (rsRounded >= 70) meta = { label: "S", color: "var(--mtp-neutral)", tip: "STRONG" };
  else if (rsRounded >= 50) meta = { label: "A", color: "#F59E0B", tip: "AVERAGE" };
  else meta = { label: "↓", color: "var(--mtp-danger)", tip: "LAGGARD — Mark: uzak dur" };

  return (
    <span
      style={{
        display: "flex",
        alignItems: "center",
        gap: 4,
        width: "100%",
        justifyContent: "flex-end",
      }}
    >
      <span style={{ fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
        {rsRounded}
      </span>
      <span
        title={`${meta.tip} (RS ${rsRounded})`}
        style={{
          fontSize: 9,
          fontWeight: 700,
          padding: "0 4px",
          borderRadius: 3,
          background: meta.color,
          color: "#fff",
        }}
      >
        {meta.label}
      </span>
    </span>
  );
}
