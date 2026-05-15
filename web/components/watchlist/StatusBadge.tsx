const STATUS_CONFIG: Record<string, { label: string; bg: string; color: string }> = {
  buy:    { label: "Buy",     bg: "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)", color: "var(--mtp-excellent)" },
  focus:  { label: "Focus",   bg: "color-mix(in srgb, var(--mtp-good)      18%, transparent)", color: "var(--mtp-good)"      },
  on_deck:{ label: "On Deck", bg: "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)", color: "var(--mtp-neutral)"   },
  watch:  { label: "Watch",   bg: "transparent",                                                color: "inherit"              },
};

export function StatusBadge({ value }: { value?: string }) {
  const c = STATUS_CONFIG[value ?? ""] ?? { label: value ?? "—", bg: "transparent", color: "inherit" };
  return (
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
  );
}
