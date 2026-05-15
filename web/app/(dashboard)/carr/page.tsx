"use client";

import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const LONG_SETUPS = [
  {
    name: "Pullback",
    termKey: "pullback",
    desc: "Trend içinde geri çekilme sonrası momentum girişi",
  },
  {
    name: "Coiled Spring",
    termKey: "coiled_spring",
    desc: "Sıkışma / konsolidasyon sonrası kırılım potansiyeli",
  },
  {
    name: "Bullish Divergence",
    termKey: null,
    desc: "Fiyat düşerken momentum yükselir — reversal sinyali",
  },
  {
    name: "Blue Sky Breakout",
    termKey: null,
    desc: "Tüm zamanların yüksek seviyesinin üzerinde kırılım",
  },
  {
    name: "Bullish Base Breakout",
    termKey: null,
    desc: "Uzun konsolidasyon tabanından yukarı kırılım",
  },
];

const SHORT_SETUPS = [
  {
    name: "Rally",
    termKey: null,
    desc: "Düşüş trendinde yükseliş sonrası momentum short girişi",
  },
  {
    name: "Coiled Spring (Short)",
    termKey: "coiled_spring",
    desc: "Sıkışma sonrası aşağı kırılım potansiyeli",
  },
  {
    name: "Bearish Divergence",
    termKey: null,
    desc: "Fiyat yükselirken momentum düşer — reversal sinyali",
  },
  {
    name: "Gap Down",
    termKey: null,
    desc: "Gap sonrası devam hareketi ve kapanış bölgesinden short",
  },
  {
    name: "Bearish Base Breakdown",
    termKey: null,
    desc: "Uzun konsolidasyon tabanından aşağı kırılım",
  },
];

function SetupCard({
  name,
  termKey,
  desc,
}: {
  name: string;
  termKey: string | null;
  desc: string;
}) {
  return (
    <Card className="flex flex-col gap-1">
      <CardHeader className="pb-1">
        <CardTitle className="text-sm font-semibold flex items-center justify-between">
          {termKey ? (
            <TermTooltip termKey={termKey}>{name}</TermTooltip>
          ) : (
            name
          )}
          <Badge variant="outline" className="text-xs font-normal">
            Yakında
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <CardDescription className="text-xs leading-relaxed">
          {desc}
        </CardDescription>
      </CardContent>
    </Card>
  );
}

export default function CarrPage() {
  return (
    <div className="p-6 flex flex-col gap-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">
          Carr Stratejisi
        </h1>
        <p className="text-sm text-muted-foreground">
          Trend Trading for a Living — Long/Short Setup&apos;lar
        </p>
      </div>

      <div className="grid md:grid-cols-2 gap-6">
        <section className="flex flex-col gap-3">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <span
              className="h-2 w-2 rounded-full"
              style={{ backgroundColor: "var(--mtp-excellent)" }}
            />
            Long Setup&apos;lar
          </h2>
          <div className="flex flex-col gap-2">
            {LONG_SETUPS.map((s) => (
              <SetupCard key={s.name} {...s} />
            ))}
          </div>
        </section>

        <section className="flex flex-col gap-3">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <span
              className="h-2 w-2 rounded-full"
              style={{ backgroundColor: "var(--mtp-danger)" }}
            />
            Short Setup&apos;lar
          </h2>
          <div className="flex flex-col gap-2">
            {SHORT_SETUPS.map((s) => (
              <SetupCard key={s.name} {...s} />
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
