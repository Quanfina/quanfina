/**
 * MindsetCardWidget (KARAR ADAY #720 + P28/P29) — Dashboard günlük Mark kartı.
 *
 * getTodayMindsetCard (deterministik) + Okudum persistence + streak + rastgele yenile.
 */
import { describe, it, expect, beforeEach, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MindsetCardWidget } from "@/components/dashboard/MindsetCard";
import { getTodayMindsetCard } from "@/data/mindset-cards";

beforeEach(() => {
  localStorage.clear();
});

describe("MindsetCardWidget — bugünün kartı render", () => {
  it("Bugünün Mark hatırlatması başlığı + quote render", () => {
    render(<MindsetCardWidget />);
    expect(screen.getByText("Bugünün Mark Hatırlatması")).toBeInTheDocument();
    const today = getTodayMindsetCard();
    // Quote blockquote içinde (tırnak ile sarılı)
    expect(screen.getByText(new RegExp(today.quote.slice(0, 20).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")))).toBeInTheDocument();
  });

  it("Kaynak referansı 'Mark Minervini' + source render", () => {
    render(<MindsetCardWidget />);
    expect(screen.getByText(/Mark Minervini/)).toBeInTheDocument();
  });

  it("Quanfina uygulama notu render", () => {
    render(<MindsetCardWidget />);
    expect(screen.getByText(/Quanfina'da:/)).toBeInTheDocument();
  });
});

describe("MindsetCardWidget — Okudum persistence (P28)", () => {
  it("İlk açılış → 'Okudum, anladım' butonu görünür", () => {
    render(<MindsetCardWidget />);
    expect(screen.getByText("Okudum, anladım")).toBeInTheDocument();
  });

  it("Okudum tıkla → 'Bugün için okundu' mesajı", async () => {
    render(<MindsetCardWidget />);
    fireEvent.click(screen.getByText("Okudum, anladım"));
    await waitFor(() =>
      expect(screen.getByText(/Bugün için okundu/)).toBeInTheDocument()
    );
    // Buton kaybolur
    expect(screen.queryByText("Okudum, anladım")).not.toBeInTheDocument();
  });

  it("localStorage'da bugün okundu kayıtlı → açılışta 'okundu' mesajı", async () => {
    const today = getTodayMindsetCard();
    const todayStr = new Date().toISOString().slice(0, 10);
    localStorage.setItem(
      "quanfina-mindset-read",
      JSON.stringify({ date: todayStr, cardId: today.id, history: [todayStr] })
    );
    render(<MindsetCardWidget />);
    await waitFor(() =>
      expect(screen.getByText(/Bugün için okundu/)).toBeInTheDocument()
    );
  });
});

describe("MindsetCardWidget — Streak rozet (P29)", () => {
  it("Streak >= 2 → '🔥 N gün' rozet görünür", async () => {
    const today = getTodayMindsetCard();
    const todayStr = new Date().toISOString().slice(0, 10);
    const y1 = new Date(); y1.setDate(y1.getDate() - 1);
    const y2 = new Date(); y2.setDate(y2.getDate() - 2);
    const history = [
      y2.toISOString().slice(0, 10),
      y1.toISOString().slice(0, 10),
      todayStr,
    ];
    localStorage.setItem(
      "quanfina-mindset-read",
      JSON.stringify({ date: todayStr, cardId: today.id, history })
    );
    render(<MindsetCardWidget />);
    await waitFor(() => expect(screen.getByText(/3 gün/)).toBeInTheDocument());
  });

  it("Streak < 2 → rozet YOK", () => {
    render(<MindsetCardWidget />);
    expect(screen.queryByText(/gün$/)).not.toBeInTheDocument();
  });
});

describe("MindsetCardWidget — rastgele yenile (manual pick)", () => {
  it("Yenile butonu → başlık 'Mark Hatırlatması' (Bugünün değil)", async () => {
    render(<MindsetCardWidget />);
    const refreshBtn = screen.getByLabelText("Rastgele kart");
    fireEvent.click(refreshBtn);
    await waitFor(() =>
      expect(screen.getByText("Mark Hatırlatması")).toBeInTheDocument()
    );
    // Manual pick → "Bugüne dön" butonu görünür
    expect(screen.getByText("Bugüne dön")).toBeInTheDocument();
  });

  it("Manual pick'te 'Okudum' butonu gizli (sadece bugünün kartında)", async () => {
    render(<MindsetCardWidget />);
    fireEvent.click(screen.getByLabelText("Rastgele kart"));
    await waitFor(() =>
      expect(screen.getByText("Mark Hatırlatması")).toBeInTheDocument()
    );
    expect(screen.queryByText("Okudum, anladım")).not.toBeInTheDocument();
  });

  it("'Bugüne dön' → tekrar bugünün kartı + 'Okudum' butonu döner", async () => {
    render(<MindsetCardWidget />);
    fireEvent.click(screen.getByLabelText("Rastgele kart"));
    await waitFor(() => expect(screen.getByText("Bugüne dön")).toBeInTheDocument());
    fireEvent.click(screen.getByText("Bugüne dön"));
    await waitFor(() =>
      expect(screen.getByText("Bugünün Mark Hatırlatması")).toBeInTheDocument()
    );
    expect(screen.getByText("Okudum, anladım")).toBeInTheDocument();
  });
});
