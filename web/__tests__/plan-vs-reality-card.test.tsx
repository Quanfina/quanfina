/**
 * PlanVsRealityCard (KARAR #725) — trade kapanışında plan disiplini denetimi.
 *
 * Stop disiplini (exit vs plan_stop) + Hedef disiplini (exit vs plan_target).
 * Props-based (trade + exitPrice string), hook yok.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { PlanVsRealityCard } from "@/components/journal/PlanVsRealityCard";
import type { Trade } from "@/types/trade";

function makeTrade(overrides: Partial<Trade> = {}): Trade {
  return {
    id: 1,
    symbol: "AAPL",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-05-20",
    entry_price: 100,
    exit_date: "2026-05-28",
    exit_price: 110,
    shares: 100,
    status: "closed",
    pl_dollar: 1000,
    pl_pct: 10,
    grade: "A",
    exit_reason: "target_hit",
    lessons: null,
    plan_stop: 95,
    plan_target: 120,
    ...overrides,
  };
}

describe("PlanVsRealityCard — gösterilmeme koşulları", () => {
  it("plan_stop=null + plan_target=null → null (Migration 008 öncesi trade)", () => {
    const { container } = render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: null, plan_target: null })}
        exitPrice="110"
      />
    );
    expect(container.firstChild).toBeNull();
  });

  it("exitPrice boş → 'Çıkış fiyatı girilince' mesajı", () => {
    render(<PlanVsRealityCard trade={makeTrade()} exitPrice="" />);
    expect(screen.getByText(/Çıkış fiyatı girilince/)).toBeInTheDocument();
  });

  it("exitPrice geçersiz (NaN) → 'Çıkış fiyatı girilince' mesajı", () => {
    render(<PlanVsRealityCard trade={makeTrade()} exitPrice="abc" />);
    expect(screen.getByText(/Çıkış fiyatı girilince/)).toBeInTheDocument();
  });
});

describe("PlanVsRealityCard — Stop disiplini", () => {
  it("exit >= stop → 'plan korundu' (ok)", () => {
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: 95, plan_target: null })}
        exitPrice="110"
      />
    );
    expect(screen.getByText(/plan korundu/)).toBeInTheDocument();
  });

  it("exit stop'un %3 toleransında altı → 'Slippage' (warn)", () => {
    // stop=95, exit=93 → 93 >= 95*0.97=92.15 → warn
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: 95, plan_target: null })}
        exitPrice="93"
      />
    );
    expect(screen.getByText(/Slippage/)).toBeInTheDocument();
  });

  it("exit stop'un çok altında → 'Disiplin ihlali' (violation)", () => {
    // stop=95, exit=85 → 85 < 95*0.97 → violation
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: 95, plan_target: null })}
        exitPrice="85"
      />
    );
    expect(screen.getByText(/Disiplin ihlali/)).toBeInTheDocument();
  });

  it("Stop Disiplini label render", () => {
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: 95, plan_target: null })}
        exitPrice="110"
      />
    );
    expect(screen.getByText("Stop Disiplini")).toBeInTheDocument();
  });
});

describe("PlanVsRealityCard — Hedef disiplini", () => {
  it("exit >= target → 'Hedefe ulaşıldı' (ok)", () => {
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: null, plan_target: 120 })}
        exitPrice="125"
      />
    );
    expect(screen.getByText(/Hedefe ulaşıldı/)).toBeInTheDocument();
  });

  it("exit target'a yakın (%85+) → 'Hedef yaklaşıldı' (info)", () => {
    // target=120, exit=110 → 110 >= 120*0.85=102 → info
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: null, plan_target: 120 })}
        exitPrice="110"
      />
    );
    expect(screen.getByText(/Hedef yaklaşıldı/)).toBeInTheDocument();
  });

  it("exit hedeften uzak (<%85) → 'Hedeften uzak çıkış' (warn)", () => {
    // target=120, exit=95 → 95 < 120*0.85=102 → warn
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: null, plan_target: 120 })}
        exitPrice="95"
      />
    );
    expect(screen.getByText(/Hedeften uzak çıkış/)).toBeInTheDocument();
  });

  it("Hedef Disiplini label render", () => {
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: null, plan_target: 120 })}
        exitPrice="125"
      />
    );
    expect(screen.getByText("Hedef Disiplini")).toBeInTheDocument();
  });
});

describe("PlanVsRealityCard — kombine (stop + hedef birlikte)", () => {
  it("Her ikisi de tanımlı → 2 satır (Stop + Hedef Disiplini)", () => {
    render(
      <PlanVsRealityCard
        trade={makeTrade({ plan_stop: 95, plan_target: 120 })}
        exitPrice="125"
      />
    );
    expect(screen.getByText("Stop Disiplini")).toBeInTheDocument();
    expect(screen.getByText("Hedef Disiplini")).toBeInTheDocument();
  });
});
