/**
 * AlertHistoryCard (P168) — Dashboard alert history timeline (24h FIFO 50).
 *
 * localStorage `position-alerts-history` + SSR hydration + relative time + severity icon.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { AlertHistoryCard } from "@/components/dashboard/AlertHistoryCard";
import type { AlertHistoryEntry } from "@/hooks/use-position-alerts";

const HISTORY_KEY = "position-alerts-history";

function makeEntry(overrides: Partial<AlertHistoryEntry> = {}): AlertHistoryEntry {
  return {
    id: `1-stop_hit-2026-05-28`,
    tradeId: 1,
    symbol: "AAPL",
    severity: "critical",
    type: "stop_hit",
    title: "🔴 AAPL — STOP'A DEĞDİ",
    message: "Canlı $92 ≤ plan stop $93. Mark TLSMW Ch 12: pozisyonu kapat.",
    current_price: 92,
    ref_price: 93,
    timestamp: Date.now() - 5 * 60_000, // 5 dk önce
    ...overrides,
  };
}

beforeEach(() => {
  localStorage.clear();
  vi.useFakeTimers({ shouldAdvanceTime: true });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("AlertHistoryCard — boş durum", () => {
  it("localStorage boş → 'Son 24 saatte uyarı tetiklenmedi'", async () => {
    render(<AlertHistoryCard />);
    await waitFor(() =>
      expect(screen.getByText(/Son 24 saatte uyarı tetiklenmedi/)).toBeInTheDocument()
    );
  });

  it("Boş durumda 'Temizle' butonu YOK", async () => {
    render(<AlertHistoryCard />);
    await waitFor(() =>
      expect(screen.getByText(/Son 24 saatte uyarı tetiklenmedi/)).toBeInTheDocument()
    );
    expect(screen.queryByText("Temizle")).not.toBeInTheDocument();
  });
});

describe("AlertHistoryCard — entry listesi", () => {
  it("1 entry → title + symbol render", async () => {
    localStorage.setItem(HISTORY_KEY, JSON.stringify([makeEntry()]));
    render(<AlertHistoryCard />);
    await waitFor(() =>
      expect(screen.getByText(/STOP'A DEĞDİ/)).toBeInTheDocument()
    );
    // AAPL hem title hem footer'da → getAllByText (en az 1 eşleşme)
    expect(screen.getAllByText(/AAPL/).length).toBeGreaterThan(0);
  });

  it("3 entry → 3 li öğesi", async () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([
        makeEntry({ id: "1-stop_hit-x", title: "AAPL stop", symbol: "AAPL" }),
        makeEntry({ id: "2-target_near-x", title: "NVDA hedef", symbol: "NVDA", severity: "info" }),
        makeEntry({ id: "3-minervini_7pct-x", title: "MSFT %7", symbol: "MSFT", severity: "warning" }),
      ])
    );
    const { container } = render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText(/AAPL stop/)).toBeInTheDocument());
    const items = container.querySelectorAll("ul li");
    expect(items).toHaveLength(3);
  });

  it("entry varsa 'Temizle' butonu görünür", async () => {
    localStorage.setItem(HISTORY_KEY, JSON.stringify([makeEntry()]));
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText("Temizle")).toBeInTheDocument());
  });
});

describe("AlertHistoryCard — Temizle butonu", () => {
  it("Temizle tıkla → localStorage silinir + boş duruma döner", async () => {
    localStorage.setItem(HISTORY_KEY, JSON.stringify([makeEntry()]));
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText("Temizle")).toBeInTheDocument());

    fireEvent.click(screen.getByText("Temizle"));

    expect(localStorage.getItem(HISTORY_KEY)).toBeNull();
    await waitFor(() =>
      expect(screen.getByText(/Son 24 saatte uyarı tetiklenmedi/)).toBeInTheDocument()
    );
  });
});

describe("AlertHistoryCard — relative time format", () => {
  it("5 dk önce → '5 dk önce' metni", async () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([makeEntry({ timestamp: Date.now() - 5 * 60_000 })])
    );
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText(/5 dk önce/)).toBeInTheDocument());
  });

  it("2 saat önce → '2 sa önce' metni", async () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([makeEntry({ timestamp: Date.now() - 2 * 3600_000 })])
    );
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText(/2 sa önce/)).toBeInTheDocument());
  });

  it("30 saniye önce → 'şimdi'", async () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([makeEntry({ timestamp: Date.now() - 30_000 })])
    );
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText(/şimdi/)).toBeInTheDocument());
  });
});

describe("AlertHistoryCard — severity ikonları (3 tip)", () => {
  it("Her severity tipinde entry render edilir (critical/warning/info)", async () => {
    localStorage.setItem(
      HISTORY_KEY,
      JSON.stringify([
        makeEntry({ id: "c", title: "Critical alert", severity: "critical" }),
        makeEntry({ id: "w", title: "Warning alert", severity: "warning" }),
        makeEntry({ id: "i", title: "Info alert", severity: "info" }),
      ])
    );
    render(<AlertHistoryCard />);
    await waitFor(() => expect(screen.getByText("Critical alert")).toBeInTheDocument());
    expect(screen.getByText("Warning alert")).toBeInTheDocument();
    expect(screen.getByText("Info alert")).toBeInTheDocument();
  });
});
