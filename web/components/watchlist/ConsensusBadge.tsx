import { Layers } from "lucide-react";

export function ConsensusBadge({ value }: { value?: number }) {
  const count = value ?? 0;
  const isMulti = count >= 2;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        fontFamily: "var(--font-jetbrains-mono, monospace)",
        fontVariantNumeric: "tabular-nums",
        fontWeight: isMulti ? 700 : 400,
        color: isMulti ? "var(--mtp-excellent)" : "inherit",
      }}
    >
      {count}
      {isMulti && <Layers size={11} strokeWidth={2} />}
    </span>
  );
}
