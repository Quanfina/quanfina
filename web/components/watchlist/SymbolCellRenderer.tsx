"use client";

import Link from "next/link";

export function SymbolCellRenderer({ value }: { value?: string }) {
  if (!value) return <span>—</span>;
  return (
    <Link
      href={`/hisse/${value}`}
      className="font-bold hover:underline hover:text-foreground transition-colors"
      style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
    >
      {value}
    </Link>
  );
}
