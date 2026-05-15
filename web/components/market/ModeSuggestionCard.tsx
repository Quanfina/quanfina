import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

const MODE_CONFIG: Record<string, { color: string; desc: string }> = {
  LONG: {
    color: "var(--mtp-excellent)",
    desc: "Piyasa bull fazında — Long setuplara odaklan",
  },
  SHORT: {
    color: "var(--mtp-danger)",
    desc: "Piyasa bear fazında — Short setuplara odaklan",
  },
  TÜMÜ: {
    color: "var(--mtp-neutral)",
    desc: "Karma piyasa — Her iki yön dikkatli değerlendir",
  },
};

interface ModeSuggestionCardProps {
  mode: string;
}

export function ModeSuggestionCard({ mode }: ModeSuggestionCardProps) {
  const config = MODE_CONFIG[mode] ?? {
    color: "var(--mtp-neutral)",
    desc: "Mod belirleniyor...",
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">Mod Önerisi</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        <div className="flex items-center gap-3">
          <div
            className="h-3 w-3 rounded-full"
            style={{ backgroundColor: config.color }}
          />
          <Badge
            variant="outline"
            className="text-sm font-bold px-3 py-1"
            style={{ color: config.color, borderColor: config.color }}
          >
            {mode}
          </Badge>
        </div>
        <p className="text-xs text-muted-foreground leading-relaxed">
          {config.desc}
        </p>
        <p className="text-xs text-muted-foreground">
          <span className="font-medium">Not:</span> Manuel override mümkün — AÇIK KONU #47
        </p>
      </CardContent>
    </Card>
  );
}
