/**
 * SignalCard (KARAR #469) + ConsensusHighlight — Sinyaller sayfası kartları.
 *
 * SignalCard props-based: symbol + strateji + statü + price + RS + 2 aksiyon.
 * ConsensusHighlight: konsensus sayım + renk.
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { SignalCard } from "@/components/signals/SignalCard";
import { ConsensusHighlight } from "@/components/signals/ConsensusHighlight";
import type { Signal } from "@/types/signal";

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => (
    <a href={href}>{children}</a>
  ),
}));

function makeSignal(overrides: Partial<Signal> = {}): Signal {
  return {
    symbol: "AAPL",
    strategy: "minervini",
    status: "focus",
    setup_type: "VCP",
    price: 150.25,
    rs_rating: 87.6,
    is_new_today: false,
    ...overrides,
  } as Signal;
}

describe("SignalCard — render içeriği", () => {
  it("symbol render", () => {
    render(<SignalCard signal={makeSignal()} onTradeClick={vi.fn()} />);
    expect(screen.getByText("AAPL")).toBeInTheDocument();
  });

  it("is_new_today=true → 'YENİ BUGÜN' badge", () => {
    render(<SignalCard signal={makeSignal({ is_new_today: true })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("YENİ BUGÜN")).toBeInTheDocument();
  });

  it("is_new_today=false → badge yok", () => {
    render(<SignalCard signal={makeSignal({ is_new_today: false })} onTradeClick={vi.fn()} />);
    expect(screen.queryByText("YENİ BUGÜN")).not.toBeInTheDocument();
  });

  it("strategy='minervini' → 'Minervini' label", () => {
    render(<SignalCard signal={makeSignal({ strategy: "minervini" })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("Minervini")).toBeInTheDocument();
  });

  it("status='focus' → 'Focus' label", () => {
    render(<SignalCard signal={makeSignal({ status: "focus" })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("Focus")).toBeInTheDocument();
  });

  it("setup_type='VCP' → render", () => {
    render(<SignalCard signal={makeSignal({ setup_type: "VCP" })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("VCP")).toBeInTheDocument();
  });

  it("price=150.25 → '$150.25' format", () => {
    render(<SignalCard signal={makeSignal({ price: 150.25 })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("$150.25")).toBeInTheDocument();
  });

  it("rs_rating=87.6 → '88' (round)", () => {
    render(<SignalCard signal={makeSignal({ rs_rating: 87.6 })} onTradeClick={vi.fn()} />);
    expect(screen.getByText("88")).toBeInTheDocument();
  });

  it("Hisse Detayı link → /hisse/AAPL", () => {
    const { container } = render(<SignalCard signal={makeSignal()} onTradeClick={vi.fn()} />);
    const link = container.querySelector('a[href="/hisse/AAPL"]');
    expect(link).not.toBeNull();
  });
});

describe("SignalCard — Trade Aç callback", () => {
  it("Trade Aç tıklanınca onTradeClick(signal) çağrılır", () => {
    const onTradeClick = vi.fn();
    const sig = makeSignal();
    render(<SignalCard signal={sig} onTradeClick={onTradeClick} />);
    fireEvent.click(screen.getByText("Trade Aç"));
    expect(onTradeClick).toHaveBeenCalledTimes(1);
    expect(onTradeClick).toHaveBeenCalledWith(sig);
  });
});

describe("ConsensusHighlight — konsensus sayım", () => {
  it("count=1 maxCount=2 → '1/2'", () => {
    render(<ConsensusHighlight count={1} maxCount={2} />);
    expect(screen.getByText("1/2")).toBeInTheDocument();
  });

  it("count=2 → '2/2' (default maxCount=2)", () => {
    render(<ConsensusHighlight count={2} />);
    expect(screen.getByText("2/2")).toBeInTheDocument();
  });

  it("count=3 maxCount=3 → '3/3'", () => {
    render(<ConsensusHighlight count={3} maxCount={3} />);
    expect(screen.getByText("3/3")).toBeInTheDocument();
  });

  it("'Konsensus' etiketi her zaman render", () => {
    render(<ConsensusHighlight count={2} />);
    expect(screen.getByText("Konsensus")).toBeInTheDocument();
  });
});
