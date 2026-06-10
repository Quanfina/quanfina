import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

// P446 (review kontrat fix — Kural #24): backend _compute_market_health
// suggested_mode ∈ {LONG, CAUTION, DEFENSIVE} döndürür (api/main.py:1308-1333).
// Eski MODE_CONFIG LONG/SHORT/TÜMÜ idi → CAUTION/DEFENSIVE eksikti; piyasa baskı
// altındayken (tam da kritik an) kart ölü "Mod belirleniyor..."e düşüyordu.
// SHORT/TÜMÜ backend'de hiç üretilmiyor (ölü kod, Kural #18). Objektif-ayna dil
// (İLKE #11): aksiyon direktifi.
const MODE_CONFIG: Record<string, { color: string; desc: string }> = {
  LONG: {
    color: "var(--mtp-excellent)",
    desc: "Yeni alım serbest — A+ setuplara odaklan",
  },
  CAUTION: {
    color: "#F59E0B",
    desc: "Yeni alım sıkı kriter — sadece lider, mevcut pozisyonları koru",
  },
  DEFENSIVE: {
    color: "var(--mtp-danger)",
    desc: "Yeni AL YASAK — sadece SAT/STOP, nakit öncelik",
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
        <CardTitle className="text-sm font-semibold">Piyasa Modu Önerisi</CardTitle>
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
      </CardContent>
    </Card>
  );
}
