/**
 * Badge komponentleri grup testi (props-based, hook yok):
 *   StatusBadge (watchlist 4 status) + TradeStatusBadge (açık/kapalı) +
 *   GradeBadge (A+..F) + ConsensusBadge (sayı + multi).
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { StatusBadge } from "@/components/watchlist/StatusBadge";
import { TradeStatusBadge } from "@/components/journal/TradeStatusBadge";
import { GradeBadge } from "@/components/journal/GradeBadge";
import { ConsensusBadge } from "@/components/watchlist/ConsensusBadge";

describe("StatusBadge — watchlist 4 katman", () => {
  it.each<[string, string]>([
    ["buy", "Buy"],
    ["focus", "Focus"],
    ["on_deck", "On Deck"],
    ["watch", "Watch"],
  ])("status='%s' → '%s' label", (value, label) => {
    render(<StatusBadge value={value} />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("Bilinmeyen status → raw value fallback", () => {
    render(<StatusBadge value="custom" />);
    expect(screen.getByText("custom")).toBeInTheDocument();
  });

  it("undefined → '—' fallback", () => {
    render(<StatusBadge />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

describe("TradeStatusBadge — açık/kapalı", () => {
  it("value='open' → 'Açık'", () => {
    render(<TradeStatusBadge value="open" />);
    expect(screen.getByText("Açık")).toBeInTheDocument();
  });

  it("value='closed' → 'Kapalı'", () => {
    render(<TradeStatusBadge value="closed" />);
    expect(screen.getByText("Kapalı")).toBeInTheDocument();
  });

  it("undefined → 'Kapalı' (open değil)", () => {
    render(<TradeStatusBadge />);
    expect(screen.getByText("Kapalı")).toBeInTheDocument();
  });
});

describe("GradeBadge — A+..F + boş", () => {
  it.each<string>(["A+", "A", "B", "C", "D", "F"])(
    "grade='%s' → label render",
    (grade) => {
      render(<GradeBadge value={grade} />);
      expect(screen.getByText(grade)).toBeInTheDocument();
    }
  );

  it("null → '—'", () => {
    render(<GradeBadge value={null} />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("undefined → '—'", () => {
    render(<GradeBadge />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("Bilinmeyen grade → raw value fallback", () => {
    render(<GradeBadge value="X" />);
    expect(screen.getByText("X")).toBeInTheDocument();
  });

  it("A+ → yeşil arka plan (#28A745) + beyaz yazı", () => {
    const { container } = render(<GradeBadge value="A+" />);
    const span = container.querySelector("span");
    expect(span?.getAttribute("style")).toContain("rgb(40, 167, 69)"); // #28A745
  });

  // P405: Mark canon tooltip + a11y
  it("A+ → tooltip Mark canon BP/SP referansı + KARAR #445", () => {
    render(<GradeBadge value="A+" />);
    const badge = screen.getByTestId("grade-badge-A+");
    const tooltip = badge.getAttribute("title");
    expect(tooltip).toContain("Mükemmel");
    expect(tooltip).toContain("KARAR #445");
    // a11y: aria-label da aynı içerik
    expect(badge.getAttribute("aria-label")).toBe(tooltip);
  });

  it("F → 'Wall' + Mark TLSMW kritik referans tooltip", () => {
    render(<GradeBadge value="F" />);
    const badge = screen.getByTestId("grade-badge-F");
    const tooltip = badge.getAttribute("title");
    expect(tooltip).toContain("Wall");
    expect(tooltip).toContain("TLSMW");
  });

  it("Her grade için cursor:help UX hint", () => {
    render(<GradeBadge value="B" />);
    const badge = screen.getByTestId("grade-badge-B");
    expect(badge.getAttribute("style")).toContain("cursor: help");
  });

  it("Tüm 6 grade için tooltip non-empty + ASLA motivasyon dili (İLKE #11)", () => {
    const grades = ["A+", "A", "B", "C", "D", "F"];
    const yagcilikYasak = ["aferin", "tebrikler", "üzülme", "harika", "kutlama"];
    for (const g of grades) {
      const { unmount } = render(<GradeBadge value={g} />);
      const badge = screen.getByTestId(`grade-badge-${g}`);
      const tooltip = badge.getAttribute("title") ?? "";
      expect(tooltip.length).toBeGreaterThan(10);
      for (const yagci of yagcilikYasak) {
        expect(tooltip.toLowerCase()).not.toContain(yagci);
      }
      unmount();
    }
  });
});

describe("ConsensusBadge — konsensus sayım", () => {
  it("value=1 → '1' (multi değil, Layers ikonu yok)", () => {
    const { container } = render(<ConsensusBadge value={1} />);
    expect(screen.getByText("1")).toBeInTheDocument();
    // Layers SVG yok
    expect(container.querySelector("svg")).toBeNull();
  });

  it("value=2 → '2' + Layers ikonu (multi)", () => {
    const { container } = render(<ConsensusBadge value={2} />);
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(container.querySelector("svg")).not.toBeNull();
  });

  it("value=3 → multi (≥2)", () => {
    const { container } = render(<ConsensusBadge value={3} />);
    expect(container.querySelector("svg")).not.toBeNull();
  });

  it("undefined → '0' (default)", () => {
    render(<ConsensusBadge />);
    expect(screen.getByText("0")).toBeInTheDocument();
  });
});
