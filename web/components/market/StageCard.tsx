import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const STAGE_COLORS: Record<number, string> = {
  1: "var(--mtp-danger)",
  2: "var(--mtp-excellent)",
  3: "var(--mtp-neutral)",
  4: "var(--mtp-danger)",
};

const STAGE_LABELS: Record<number, string> = {
  1: "Stage 1 — Baz",
  2: "Stage 2 — Yükseliş",
  3: "Stage 3 — Tepe",
  4: "Stage 4 — Düşüş",
};

interface StageCardProps {
  symbol: string;
  stage: number;
}

export function StageCard({ symbol, stage }: StageCardProps) {
  const color = STAGE_COLORS[stage] ?? "var(--mtp-neutral)";
  const label = STAGE_LABELS[stage] ?? `Stage ${stage}`;

  return (
    <Card className="flex flex-col">
      <CardHeader className="pb-1">
        <CardTitle className="text-sm font-semibold flex items-center justify-between">
          <TermTooltip termKey="spy_stage_2">{symbol}</TermTooltip>
          <span
            className="text-xs font-normal px-2 py-0.5 rounded-full"
            style={{
              background: `color-mix(in srgb, ${color} 15%, transparent)`,
              color,
            }}
          >
            {label}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-2">
          <div
            className="h-2 w-2 rounded-full"
            style={{ backgroundColor: color }}
          />
          <span className="text-xs text-muted-foreground">
            {stage === 2
              ? "Bull fazında — LONG mod destekleniyor"
              : stage === 1
              ? "Konsolidasyon — Bekleme modu"
              : stage === 3
              ? "Tepe bölgesi — Dikkatli ol"
              : "Bear fazında — SHORT mod"}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}
