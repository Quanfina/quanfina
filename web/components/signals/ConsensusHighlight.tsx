"use client";

import { Layers } from "lucide-react";

interface Props {
  count: number;
  maxCount?: number;
}

const COLOR: Record<number, string> = {
  1: "var(--muted-foreground)",
  2: "#4B9CD3",
  3: "#28A745",
};

export function ConsensusHighlight({ count, maxCount = 2 }: Props) {
  const color = COLOR[count] ?? "#28A745";

  return (
    <div className="flex flex-col items-center gap-0.5 min-w-[52px]">
      <div className="flex items-center gap-1">
        <Layers size={13} style={{ color }} />
        <span style={{ fontSize: 22, fontWeight: 700, lineHeight: 1, color }}>
          {count}/{maxCount}
        </span>
      </div>
      <span
        style={{
          fontSize: 9,
          color,
          fontWeight: 700,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
        }}
      >
        Konsensus
      </span>
    </div>
  );
}
