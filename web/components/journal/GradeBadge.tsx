type Config = { bg: string; color: string; label: string };

const GRADE_CONFIG: Record<string, Config> = {
  "A+": { bg: "#28A745", color: "#ffffff", label: "A+" },
  "A":  { bg: "#90EE90", color: "#1a1a1a", label: "A"  },
  "B":  { bg: "#4B9CD3", color: "#ffffff", label: "B"  },
  "C":  { bg: "#FFDA0D", color: "#1a1a1a", label: "C"  },
  "D":  { bg: "#FF5733", color: "#ffffff", label: "D"  },
  "F":  { bg: "#D70040", color: "#ffffff", label: "F"  },
};

export function GradeBadge({ value }: { value?: string | null }) {
  if (!value) {
    return (
      <span style={{ color: "var(--muted-foreground)", fontSize: 12 }}>—</span>
    );
  }
  const c = GRADE_CONFIG[value];
  if (!c) {
    return <span style={{ fontSize: 12 }}>{value}</span>;
  }
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        minWidth: 28,
        padding: "2px 8px",
        borderRadius: 4,
        background: c.bg,
        color: c.color,
        fontSize: 12,
        fontWeight: 700,
        letterSpacing: "0.02em",
      }}
    >
      {c.label}
    </span>
  );
}
