/**
 * GradeBadge — Mark TradeGrader 6 harf (A+, A, B, C, D, F) görsel rozet.
 *
 * P405: Mark canon tooltip eklendi (KARAR #445 + EK10 TradeGrader 17 kategori).
 * Sn. Ferit grade üzerine geldiğinde Mark canon kategorisi + kısa açıklama
 * görür. Yağcılık YOK (İLKE #11 Objektif Ayna Dil) — somut sebep + Mark referans.
 * a11y: title + aria-label + cursor:help (screen reader + UX hint).
 */
type Config = { bg: string; color: string; label: string; tooltip: string };

const GRADE_CONFIG: Record<string, Config> = {
  "A+": {
    bg: "#28A745", color: "#ffffff", label: "A+",
    tooltip: "A+ Mükemmel — pivot ≤%2 + stop hassasiyet (Mark TradeGrader BP/SP, KARAR #445)",
  },
  "A": {
    bg: "#90EE90", color: "#1a1a1a", label: "A",
    tooltip: "A İyi — plana uygun, küçük sapma (Mark canon)",
  },
  "B": {
    bg: "#4B9CD3", color: "#ffffff", label: "B",
    tooltip: "B Kabul edilebilir — orta sapma (Cut Loss Early veya Bought Late <%3)",
  },
  "C": {
    bg: "#FFDA0D", color: "#1a1a1a", label: "C",
    tooltip: "C Disiplin ihlali — geliştirilmesi gerek",
  },
  "D": {
    bg: "#FF5733", color: "#ffffff", label: "D",
    tooltip: "D Stop geç/erken giriş — Mark canon ihlali",
  },
  "F": {
    bg: "#D70040", color: "#ffffff", label: "F",
    tooltip: "F Wall — %10 mutlak stop ihlali (Mark TLSMW kritik, TheWall override)",
  },
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
      title={c.tooltip}
      aria-label={c.tooltip}
      data-testid={`grade-badge-${value}`}
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
        cursor: "help",
      }}
    >
      {c.label}
    </span>
  );
}
