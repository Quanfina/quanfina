"use client";

import { TermTooltip } from "@/components/terminology/TermTooltip";

const SETUP_TERM_KEY: Record<string, string> = {
  "Pullback":             "pullback",
  "Coiled Spring":        "coiled_spring",
  "VCP":                  "vcp",
};

export function SetupCellRenderer({ value }: { value?: string | null }) {
  if (!value) {
    return <span style={{ color: "var(--muted-foreground)", opacity: 0.4 }}>—</span>;
  }
  const termKey = SETUP_TERM_KEY[value];
  if (termKey) {
    return <TermTooltip termKey={termKey}>{value}</TermTooltip>;
  }
  return <span>{value}</span>;
}
