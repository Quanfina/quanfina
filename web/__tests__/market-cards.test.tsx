/**
 * Market (Piyasa Durumu) kartları: HealthScoreCard + ModeSuggestionCard +
 * StageCard + SectorSummaryCard. Props-based (useTerms mock — TermTooltip).
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { HealthScoreCard } from "@/components/market/HealthScoreCard";
import { ModeSuggestionCard } from "@/components/market/ModeSuggestionCard";
import { StageCard } from "@/components/market/StageCard";
import { SectorSummaryCard } from "@/components/market/SectorSummaryCard";
import type { SectorChange } from "@/types/market";

vi.mock("@/hooks/use-terms", () => ({
  useTerms: () => ({ data: [] }),
}));

describe("HealthScoreCard — skor + label renk (P382 clean-room enum)", () => {
  it("score=75 → '75' render", () => {
    render(<HealthScoreCard score={75} label="HEALTHY" vix={14} distributionDays={1} />);
    expect(screen.getByText("75")).toBeInTheDocument();
  });

  it("'Market Health Score' başlık", () => {
    render(<HealthScoreCard score={50} label="NEUTRAL" vix={20} distributionDays={3} />);
    expect(screen.getByText("Market Health Score")).toBeInTheDocument();
  });

  it("label='HEALTHY' → TR 'Sağlıklı' goster", () => {
    render(<HealthScoreCard score={75} label="HEALTHY" vix={14} distributionDays={1} />);
    expect(screen.getByText("Sağlıklı")).toBeInTheDocument();
  });

  it("label='NEUTRAL' → TR 'Nötr' goster", () => {
    render(<HealthScoreCard score={50} label="NEUTRAL" vix={20} distributionDays={3} />);
    expect(screen.getByText("Nötr")).toBeInTheDocument();
  });

  it("label='UNDER_PRESSURE' → TR 'Baskı Altında' goster (Mark TLSMW Bol. 5 kitap birebir)", () => {
    render(<HealthScoreCard score={25} label="UNDER_PRESSURE" vix={30} distributionDays={5} />);
    expect(screen.getByText("Baskı Altında")).toBeInTheDocument();
  });

  it.each<"HEALTHY" | "NEUTRAL" | "UNDER_PRESSURE">(["HEALTHY", "NEUTRAL", "UNDER_PRESSURE"])(
    "label='%s' → kart render eder (renk eşlemesi)",
    (label) => {
      render(<HealthScoreCard score={60} label={label} vix={18} distributionDays={2} />);
      expect(screen.getByText("60")).toBeInTheDocument();
    }
  );
});

describe("ModeSuggestionCard — LONG/SHORT/TÜMÜ", () => {
  it("'Mod Önerisi' başlık", () => {
    render(<ModeSuggestionCard mode="LONG" />);
    expect(screen.getByText("Mod Önerisi")).toBeInTheDocument();
  });

  it("mode='LONG' → bull açıklaması", () => {
    render(<ModeSuggestionCard mode="LONG" />);
    expect(screen.getByText(/bull fazında/)).toBeInTheDocument();
  });

  it("mode='SHORT' → bear açıklaması", () => {
    render(<ModeSuggestionCard mode="SHORT" />);
    expect(screen.getByText(/bear fazında/)).toBeInTheDocument();
  });

  it("mode='TÜMÜ' → karma açıklaması", () => {
    render(<ModeSuggestionCard mode="TÜMÜ" />);
    expect(screen.getByText(/Karma piyasa/)).toBeInTheDocument();
  });

  it("Bilinmeyen mode → 'Mod belirleniyor...' fallback", () => {
    render(<ModeSuggestionCard mode="XYZ" />);
    expect(screen.getByText(/Mod belirleniyor/)).toBeInTheDocument();
  });
});

describe("StageCard — 4 stage label", () => {
  it.each<[number, string]>([
    [1, "Stage 1 — Baz"],
    [2, "Stage 2 — Yükseliş"],
    [3, "Stage 3 — Tepe"],
    [4, "Stage 4 — Düşüş"],
  ])("stage=%i → '%s' label", (stage, label) => {
    render(<StageCard symbol="SPY" stage={stage} />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("symbol render (TermTooltip içinde)", () => {
    render(<StageCard symbol="QQQ" stage={2} />);
    expect(screen.getByText("QQQ")).toBeInTheDocument();
  });

  it("stage=2 → 'Bull fazında — LONG mod destekleniyor'", () => {
    render(<StageCard symbol="SPY" stage={2} />);
    expect(screen.getByText(/Bull fazında/)).toBeInTheDocument();
  });

  it("Bilinmeyen stage → 'Stage N' fallback", () => {
    render(<StageCard symbol="SPY" stage={9} />);
    expect(screen.getByText("Stage 9")).toBeInTheDocument();
  });
});

describe("SectorSummaryCard — sektör özeti", () => {
  const top: SectorChange[] = [
    { name: "Technology", change_pct: 2.5 } as SectorChange,
    { name: "Healthcare", change_pct: 1.8 } as SectorChange,
  ];
  const bottom: SectorChange[] = [
    { name: "Energy", change_pct: -3.2 } as SectorChange,
  ];

  it("'Sektör Özeti' başlık + 'En İyi'", () => {
    render(<SectorSummaryCard topSectors={top} bottomSectors={bottom} />);
    expect(screen.getByText("Sektör Özeti")).toBeInTheDocument();
    expect(screen.getByText("En İyi")).toBeInTheDocument();
  });

  it("Top sektör adı + pozitif % render", () => {
    render(<SectorSummaryCard topSectors={top} bottomSectors={bottom} />);
    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(screen.getByText("+2.5%")).toBeInTheDocument();
  });

  it("Bottom sektör negatif % render", () => {
    render(<SectorSummaryCard topSectors={top} bottomSectors={bottom} />);
    expect(screen.getByText("Energy")).toBeInTheDocument();
    expect(screen.getByText("-3.2%")).toBeInTheDocument();
  });
});
