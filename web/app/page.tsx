"use client";

import Decimal from "decimal.js";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ThemeToggle } from "@/components/layout/theme-toggle";

const MTP_COLORS = [
  { name: "Passive",   hex: "#cbcbcb" },
  { name: "Fatal",     hex: "#d70040" },
  { name: "Danger",    hex: "#ff5733" },
  { name: "Waiting",   hex: "#fdda0d" },
  { name: "Neutral",   hex: "#4b9cd3" },
  { name: "Good",      hex: "#90ee90" },
  { name: "Excellent", hex: "#28a745" },
];

const TEST_NUMBERS = [
  new Decimal("1234567.89"),
  new Decimal("0.00001234"),
  new Decimal("99.5").div(new Decimal("100")),
];

export default function Home() {
  return (
    <div className="min-h-screen p-8 flex flex-col gap-8 font-sans">
      <section className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">
          Quanfina — POC ADIM 1.3 Smoke Test
        </h1>
        <ThemeToggle />
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          Sayfalar
        </h2>
        <div className="flex gap-2 flex-wrap">
          <Link href="/minervini">
            <Button variant="outline" size="sm">Minervini Tarama</Button>
          </Link>
          <Link href="/api-test">
            <Button variant="outline" size="sm">API Test</Button>
          </Link>
        </div>
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          shadcn/ui Components
        </h2>
        <div className="flex gap-2 flex-wrap items-center">
          <Button variant="default">Default</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="destructive">Destructive</Button>
          <Badge>Badge</Badge>
          <Badge variant="outline">Outline Badge</Badge>
        </div>
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          Tipografi — JetBrains Mono (KARAR #299)
        </h2>
        <div className="font-mono tabular-nums text-sm flex flex-col gap-1 p-4 rounded-lg border bg-muted">
          {TEST_NUMBERS.map((n, i) => (
            <div key={i} className="flex gap-4">
              <span className="text-muted-foreground w-4">{i + 1}</span>
              <span>{n.toFixed(8)}</span>
              <span className="text-muted-foreground">decimal.js — float yok</span>
            </div>
          ))}
        </div>
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          MTP Durum Paleti — KARAR #308
        </h2>
        <div className="flex gap-3 flex-wrap">
          {MTP_COLORS.map((c) => (
            <div key={c.hex} className="flex flex-col items-center gap-1">
              <div
                className="w-12 h-12 rounded-md border"
                style={{ backgroundColor: c.hex }}
              />
              <span className="text-xs font-mono text-muted-foreground">{c.name}</span>
              <span className="text-xs font-mono text-muted-foreground">{c.hex}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
          Tailwind v4 Utility Test
        </h2>
        <div className="grid grid-cols-4 gap-2 text-xs font-mono">
          <div className="p-3 rounded bg-primary text-primary-foreground">primary</div>
          <div className="p-3 rounded bg-secondary text-secondary-foreground">secondary</div>
          <div className="p-3 rounded bg-muted text-muted-foreground">muted</div>
          <div className="p-3 rounded bg-accent text-accent-foreground">accent</div>
          <div className="p-3 rounded bg-card text-card-foreground border">card</div>
          <div className="p-3 rounded bg-destructive text-white">destructive</div>
          <div className="p-3 rounded border">border</div>
          <div className="p-3 rounded bg-background text-foreground border">bg/fg</div>
        </div>
      </section>
    </div>
  );
}
