import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const LABEL_COLORS: Record<string, string> = {
  YEŞİL: "var(--mtp-excellent)",
  SARI: "var(--mtp-neutral)",
  KIRMIZI: "var(--mtp-danger)",
};

interface HealthScoreCardProps {
  score: number;
  label: string;
  vix: number;
  distributionDays: number;
}

export function HealthScoreCard({
  score,
  label,
  vix,
  distributionDays,
}: HealthScoreCardProps) {
  const color = LABEL_COLORS[label] ?? "var(--mtp-neutral)";

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">
          <TermTooltip termKey="market_breadth">Market Health Score</TermTooltip>
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <span
            className="text-3xl font-bold tabular-nums"
            style={{ color }}
          >
            {score}
          </span>
          <span
            className="text-sm font-semibold px-3 py-1 rounded-full"
            style={{
              background: `color-mix(in srgb, ${color} 15%, transparent)`,
              color,
            }}
          >
            {label}
          </span>
        </div>

        <div className="h-2 rounded-full bg-muted overflow-hidden">
          <div
            className="h-full rounded-full transition-all"
            style={{
              width: `${score}%`,
              backgroundColor: color,
            }}
          />
        </div>

        <div className="grid grid-cols-2 gap-2 text-xs">
          <div className="flex flex-col gap-0.5">
            <span className="text-muted-foreground">
              <TermTooltip termKey="vix">VIX</TermTooltip>
            </span>
            <span
              className="font-mono font-semibold"
              style={{ color: vix > 25 ? "var(--mtp-danger)" : "var(--mtp-excellent)" }}
            >
              {vix.toFixed(1)}
            </span>
          </div>
          <div className="flex flex-col gap-0.5">
            <span className="text-muted-foreground">
              <TermTooltip termKey="distribution_days">Dist. Days</TermTooltip>
            </span>
            <span
              className="font-mono font-semibold"
              style={{ color: distributionDays >= 4 ? "var(--mtp-danger)" : "inherit" }}
            >
              {distributionDays}
            </span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
