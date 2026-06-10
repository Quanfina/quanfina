import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { ModBadge } from "@/components/mark/ModBadge";
import type { TradingMode, TradingModeInfo } from "@/hooks/use-trading-mode";

// next/link mock — jsdom href kontrolü için
vi.mock("next/link", () => ({
  default: ({ children, href, ...rest }: { children: React.ReactNode; href: string; [k: string]: unknown }) => (
    <a href={href} {...rest}>
      {children}
    </a>
  ),
}));

// useTradingMode mock — kontrollü mode geçişi
const mockTradingMode = vi.fn<() => TradingModeInfo>();

vi.mock("@/hooks/use-trading-mode", async () => {
  const actual = await vi.importActual<
    typeof import("@/hooks/use-trading-mode")
  >("@/hooks/use-trading-mode");
  return {
    ...actual,
    useTradingMode: () => mockTradingMode(),
  };
});

function makeMode(
  mode: TradingMode,
  overrides: Partial<TradingModeInfo> = {}
): TradingModeInfo {
  const base: TradingModeInfo = {
    mode,
    reason: `${mode} test reason`,
    emoji:
      mode === "defansif"
        ? "🛡️"
        : mode === "rehab"
        ? "🩹"
        : mode === "agresif"
        ? "🚀"
        : "●",
    color:
      mode === "defansif"
        ? "var(--mtp-danger)"
        : mode === "rehab"
        ? "#F59E0B"
        : mode === "agresif"
        ? "var(--mtp-excellent)"
        : "var(--mtp-neutral)",
    recommendedSizingPct:
      mode === "rehab" ? 0.5 : mode === "agresif" ? 1.5 : mode === "defansif" ? 0 : 1.0,
    uiBehavior: `${mode} ux behavior`,
    consecutiveWins: 0,
    consecutiveLosses: 0,
    totalClosedTrades: 0,
  };
  return { ...base, ...overrides };
}

describe("ModBadge (P193 — Vizyon İLKE #10 4-Mod görsel rozet)", () => {
  beforeEach(() => {
    mockTradingMode.mockReset();
    mockTradingMode.mockReturnValue(makeMode("normal"));
  });

  describe("variant='compact' (sidebar/header inline rozet)", () => {
    it.each<[TradingMode, string]>([
      ["normal", "NORMAL"],
      ["defansif", "DEFANSİF"],
      ["rehab", "REHAB"],
      ["agresif", "AGRESİF"],
    ])("mode='%s' → 'MOD: %s' label", (mode, label) => {
      mockTradingMode.mockReturnValue(makeMode(mode));
      render(<ModBadge variant="compact" />);
      expect(screen.getByText(new RegExp(`MOD:\\s*${label}`))).toBeInTheDocument();
    });

    it("title attribute reason field göstermeli (tooltip)", () => {
      mockTradingMode.mockReturnValue(
        makeMode("defansif", {
          reason: "Piyasa sağlığı 25/100 (<30). Yeni AL'lar bloklu.",
        })
      );
      const { container } = render(<ModBadge variant="compact" />);
      const badge = container.querySelector("span[title]");
      expect(badge).not.toBeNull();
      expect(badge?.getAttribute("title")).toContain("Piyasa sağlığı 25/100");
    });

    it("compact: link/href yok (sidebar non-navigational)", () => {
      const { container } = render(<ModBadge variant="compact" />);
      expect(container.querySelector("a")).toBeNull();
    });
  });

  describe("variant='full' (Dashboard kartı)", () => {
    it("default variant='full' → kart render", () => {
      render(<ModBadge />);
      expect(screen.getByText("Trade Modu")).toBeInTheDocument();
    });

    it("link href='/risk-yonetimi' (Mark risk sayfasına yönlendirme)", () => {
      const { container } = render(<ModBadge variant="full" />);
      const link = container.querySelector("a");
      expect(link).not.toBeNull();
      expect(link?.getAttribute("href")).toBe("/risk-yonetimi");
    });

    it("emoji + label + reason + uiBehavior hepsi render", () => {
      mockTradingMode.mockReturnValue(
        makeMode("defansif", {
          reason: "Piyasa Stage 4",
          uiBehavior: "Yeni AL'lar BLOK",
        })
      );
      render(<ModBadge variant="full" />);
      expect(screen.getByText("🛡️")).toBeInTheDocument();
      expect(screen.getByText("DEFANSİF")).toBeInTheDocument();
      expect(screen.getByText(/Piyasa Stage 4/)).toBeInTheDocument();
      expect(screen.getByText(/Yeni AL.*BLOK/)).toBeInTheDocument();
    });

    it("sizing göstergesi: normal → '%1 R'", () => {
      mockTradingMode.mockReturnValue(makeMode("normal"));
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/%1 R/)).toBeInTheDocument();
    });

    it("sizing göstergesi: rehab → '%0.5 R' (yarım pozisyon)", () => {
      mockTradingMode.mockReturnValue(makeMode("rehab"));
      render(<ModBadge variant="full" />);
      // Component '%' + (0.5).toFixed(1).replace('.0','') = '%0.5 R'
      expect(screen.getByText(/%0\.5 R/)).toBeInTheDocument();
    });

    it("sizing göstergesi: agresif → '%1.5 R' (Hot hand)", () => {
      mockTradingMode.mockReturnValue(makeMode("agresif"));
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/%1\.5 R/)).toBeInTheDocument();
    });
  });

  describe("Streak göstergesi (variant='full' footer)", () => {
    it("consecutiveWins=4 → '4 ardışık kazanç' yeşil", () => {
      mockTradingMode.mockReturnValue(
        makeMode("normal", { consecutiveWins: 4, totalClosedTrades: 10 })
      );
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/4 ardışık kazanç/)).toBeInTheDocument();
    });

    it("consecutiveLosses=3 → '3 ardışık kayıp' kırmızı (Rehab tetiği)", () => {
      mockTradingMode.mockReturnValue(
        makeMode("rehab", { consecutiveLosses: 3, totalClosedTrades: 8 })
      );
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/3 ardışık kayıp/)).toBeInTheDocument();
    });

    it("streak=0 → 'ilk trade veya nötr' mesajı", () => {
      mockTradingMode.mockReturnValue(makeMode("normal"));
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/ilk trade veya nötr/)).toBeInTheDocument();
    });

    it("totalClosedTrades footer'da göster (ml-auto)", () => {
      mockTradingMode.mockReturnValue(
        makeMode("normal", { totalClosedTrades: 42 })
      );
      render(<ModBadge variant="full" />);
      expect(screen.getByText(/42 toplam kapalı/)).toBeInTheDocument();
    });
  });
});
