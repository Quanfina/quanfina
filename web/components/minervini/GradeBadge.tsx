const GRADE_COLORS: Record<string, { bg: string; color: string }> = {
  A: { bg: "var(--mtp-excellent)", color: "#fff" },
  B: { bg: "var(--mtp-neutral)", color: "#fff" },
  C: { bg: "var(--mtp-waiting)", color: "#222" },
  D: { bg: "var(--mtp-danger)", color: "#fff" },
  F: { bg: "var(--mtp-fatal)", color: "#fff" },
};

export function GradeBadge({ value }: { value?: string }) {
  const grade = value ?? "";
  const c = GRADE_COLORS[grade] ?? { bg: "transparent", color: "inherit" };
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        minWidth: 28,
        height: 22,
        borderRadius: 4,
        padding: "2px 8px",
        background: c.bg,
        color: c.color,
        fontSize: 12,
        fontWeight: 600,
        fontFamily: "var(--font-jetbrains-mono, monospace)",
      }}
    >
      {grade || "—"}
    </span>
  );
}
