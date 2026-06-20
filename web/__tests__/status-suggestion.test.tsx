/**
 * P562 (List Degradation) — suggestStatus + StatusBadge terfi/düşüş nudge.
 *
 * 4-liste hiyerarşisi (watch→on_deck→focus→buy) Quanfina tasarımı; öneri pivot_status'tan
 * (Mark TLSMW Ch 10 canon). Advisory — otomatik değiştirmez (Kural #4). Nudge sadece mevcut≠öneri.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { suggestStatus, statusRank } from "@/lib/watchlist-status";
import { StatusBadge } from "@/components/watchlist/StatusBadge";

describe("suggestStatus (pivot → tier)", () => {
  it("CONFIRMED → buy", () => {
    expect(suggestStatus("CONFIRMED", null)).toBe("buy");
  });
  it("NEAR_PIVOT → focus", () => {
    expect(suggestStatus("NEAR_PIVOT", null)).toBe("focus");
  });
  it("WEAK → on_deck", () => {
    expect(suggestStatus("WEAK", null)).toBe("on_deck");
  });
  it("Pocket Pivot (GOOD/CANDIDATE) → on_deck (pivot sinyali yokken bile)", () => {
    expect(suggestStatus(null, "GOOD")).toBe("on_deck");
    expect(suggestStatus("BELOW_PIVOT", "CANDIDATE")).toBe("on_deck");
  });
  it("BELOW_PIVOT / null → watch", () => {
    expect(suggestStatus("BELOW_PIVOT", null)).toBe("watch");
    expect(suggestStatus(null, null)).toBe("watch");
  });
  it("statusRank hiyerarşi sırası", () => {
    expect(statusRank("watch")).toBe(0);
    expect(statusRank("buy")).toBe(3);
    expect(statusRank("bilinmeyen")).toBe(-1);
  });
});

describe("StatusBadge — terfi/düşüş nudge", () => {
  it("watch + pivot CONFIRMED → ↑Buy terfi nudge", () => {
    render(<StatusBadge data={{ status: "watch", pivot_status: "CONFIRMED" }} />);
    const n = screen.getByTestId("status-suggestion");
    expect(n.textContent).toContain("↑");
    expect(n.textContent).toContain("Buy");
  });

  it("buy + pivot BELOW_PIVOT → ↓Watch düşüş nudge", () => {
    render(<StatusBadge data={{ status: "buy", pivot_status: "BELOW_PIVOT" }} />);
    const n = screen.getByTestId("status-suggestion");
    expect(n.textContent).toContain("↓");
    expect(n.textContent).toContain("Watch");
  });

  it("focus + pivot NEAR_PIVOT → öneri = mevcut → nudge YOK", () => {
    render(<StatusBadge data={{ status: "focus", pivot_status: "NEAR_PIVOT" }} />);
    expect(screen.queryByTestId("status-suggestion")).toBeNull();
  });

  it("sadece value (data yok) → nudge YOK (graceful — direkt çağıranlar)", () => {
    render(<StatusBadge value="focus" />);
    expect(screen.queryByTestId("status-suggestion")).toBeNull();
    expect(screen.getByText("Focus")).toBeInTheDocument();
  });

  it("data ama sinyal yok (pivot/pocket null) → nudge YOK", () => {
    render(<StatusBadge data={{ status: "watch", pivot_status: null, pocket_pivot: null }} />);
    expect(screen.queryByTestId("status-suggestion")).toBeNull();
  });
});
