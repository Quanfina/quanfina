"use client";

import { TermTooltip } from "@/components/terminology/TermTooltip";

const TERM_KEY: Record<string, string> = {
  minervini: "sepa",
  carr:      "carr_method",
};

const LABELS: Record<string, string> = {
  minervini: "Minervini",
  carr:      "Carr",
};

export function StrategyCellRenderer({ value }: { value?: string }) {
  const label = LABELS[value ?? ""] ?? (value ?? "—");
  const termKey = TERM_KEY[value ?? ""];
  if (termKey) {
    return <TermTooltip termKey={termKey}>{label}</TermTooltip>;
  }
  return <span>{label}</span>;
}
