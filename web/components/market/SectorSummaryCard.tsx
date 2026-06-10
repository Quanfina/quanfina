import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { SectorChange } from "@/types/market";

interface SectorSummaryCardProps {
  topSectors: SectorChange[];
  bottomSectors: SectorChange[];
}

function SectorRow({ sector }: { sector: SectorChange }) {
  const positive = sector.change_pct >= 0;
  const color = positive ? "var(--mtp-excellent)" : "var(--mtp-danger)";
  const sign = positive ? "+" : "";

  return (
    <div className="flex items-center justify-between text-xs py-0.5">
      <span className="text-foreground truncate max-w-[140px]">{sector.name}</span>
      <span className="font-mono font-semibold" style={{ color }}>
        {sign}{sector.change_pct.toFixed(1)}%
      </span>
    </div>
  );
}

export function SectorSummaryCard({ topSectors, bottomSectors }: SectorSummaryCardProps) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">Sektör Özeti</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div>
          <p className="text-xs font-medium text-muted-foreground mb-1 uppercase tracking-wide">
            En İyi
          </p>
          {topSectors.map((s) => (
            <SectorRow key={s.name} sector={s} />
          ))}
        </div>
        <div className="border-t pt-2">
          <p className="text-xs font-medium text-muted-foreground mb-1 uppercase tracking-wide">
            En Kötü
          </p>
          {bottomSectors.map((s) => (
            <SectorRow key={s.name} sector={s} />
          ))}
        </div>
        <p className="text-xs text-muted-foreground italic">
          Detaylı sektör analizi: Sektör Rotasyonu sayfası
        </p>
      </CardContent>
    </Card>
  );
}
