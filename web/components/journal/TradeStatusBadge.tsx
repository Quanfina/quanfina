export function TradeStatusBadge({ value }: { value?: string }) {
  const isOpen = value === "open";
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 8px",
        borderRadius: 4,
        background: isOpen
          ? "color-mix(in srgb, var(--mtp-neutral) 18%, transparent)"
          : "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)",
        color: isOpen ? "var(--mtp-neutral)" : "var(--mtp-excellent)",
        fontSize: 12,
        fontWeight: 500,
      }}
    >
      {isOpen ? "Açık" : "Kapalı"}
    </span>
  );
}
