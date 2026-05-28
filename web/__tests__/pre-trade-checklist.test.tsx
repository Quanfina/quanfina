import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import {
  PreTradeChecklist,
  type PreTradeChecklistProps,
} from "@/components/journal/PreTradeChecklist";
import type { TradingMode, TradingModeInfo } from "@/hooks/use-trading-mode";

// useTradingMode mock — test başına farklı mod döndürmek için kontrollü helper
const mockTradingMode = vi.fn<[], TradingModeInfo>();

vi.mock("@/hooks/use-trading-mode", async () => {
  const actual = await vi.importActual<
    typeof import("@/hooks/use-trading-mode")
  >("@/hooks/use-trading-mode");
  return {
    ...actual,
    useTradingMode: () => mockTradingMode(),
  };
});

function makeMode(mode: TradingMode): TradingModeInfo {
  return {
    mode,
    reason: `${mode} test mode`,
    emoji: "●",
    color: "blue",
    recommendedSizingPct: mode === "rehab" ? 0.5 : mode === "agresif" ? 1.5 : 1.0,
    uiBehavior: "test",
    consecutiveWins: 0,
    consecutiveLosses: 0,
    totalClosedTrades: 0,
  };
}

// Tüm 8 koşul sağlanan happy-path props (Mark canon birebir)
const HAPPY: PreTradeChecklistProps = {
  symbol: "AAPL",
  stage: 2,
  rsRating: 85,
  vcpPass: true,
  pivotPass: false,
  planEntryTrigger: "Pivot $150.25 breakout",
  planStop: 95,
  planTarget: 115,
  entryPrice: 100,
  planSizePct: 20,
};

describe("PreTradeChecklist (P229+P265 — 8 koşul Mark canon)", () => {
  beforeEach(() => {
    mockTradingMode.mockReset();
    mockTradingMode.mockReturnValue(makeMode("normal"));
  });

  describe("Render — koşul sayısı", () => {
    it("compact=false → 8 koşul render (Stage, RS, Setup, Plan x4, Mod)", () => {
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/Stage 2 piyasa/)).toBeInTheDocument();
      expect(screen.getByText(/RS Rating ≥ 70/)).toBeInTheDocument();
      expect(screen.getByText(/VCP \/ Pivot Setup/)).toBeInTheDocument();
      expect(screen.getByText(/Plan: Giriş tetikleyicisi/)).toBeInTheDocument();
      expect(screen.getByText(/Plan: Stop loss/)).toBeInTheDocument();
      expect(screen.getByText(/Plan: Hedef R\/R ≥ 2/)).toBeInTheDocument();
      expect(screen.getByText(/Plan: Pozisyon ≤ 25%/)).toBeInTheDocument();
      expect(screen.getByText(/Mod farkındalığı/)).toBeInTheDocument();
    });

    it("compact=true → 4 koşul (Stage + RS + Setup + Mod)", () => {
      render(<PreTradeChecklist {...HAPPY} compact />);
      expect(screen.queryByText(/Plan: Stop loss/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Plan: Hedef/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Plan: Pozisyon/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Plan: Giriş tetikleyicisi/)).not.toBeInTheDocument();
      expect(screen.getByText(/Stage 2 piyasa/)).toBeInTheDocument();
      expect(screen.getByText(/Mod farkındalığı/)).toBeInTheDocument();
    });
  });

  describe("Stage koşulu (Carr/Weinstein)", () => {
    it("stage=2 → ok detail '30W MA üstü'", () => {
      render(<PreTradeChecklist {...HAPPY} stage={2} />);
      expect(screen.getByText(/30W MA üstü/)).toBeInTheDocument();
    });

    it("stage=4 → fail detail 'UZAK DUR'", () => {
      render(<PreTradeChecklist {...HAPPY} stage={4} />);
      expect(screen.getByText(/UZAK DUR/)).toBeInTheDocument();
    });

    it("stage=null → warn 'verisi yok'", () => {
      render(<PreTradeChecklist {...HAPPY} stage={null} />);
      expect(screen.getByText(/Stage verisi yok/)).toBeInTheDocument();
    });
  });

  describe("RS Rating koşulu (Mark TLSMW Leaders First)", () => {
    it("RS=85 → ok 'Leader'", () => {
      render(<PreTradeChecklist {...HAPPY} rsRating={85} />);
      expect(screen.getByText(/RS 85.*Leader/)).toBeInTheDocument();
    });

    it("RS=30 → fail 'Laggard'", () => {
      render(<PreTradeChecklist {...HAPPY} rsRating={30} />);
      expect(screen.getByText(/Laggard/)).toBeInTheDocument();
    });

    it("RS=60 → warn 'Average'", () => {
      render(<PreTradeChecklist {...HAPPY} rsRating={60} />);
      expect(screen.getByText(/Average/)).toBeInTheDocument();
    });
  });

  describe("Stop loss %7 mutlak limit (Mark TTLC s.131)", () => {
    it("entry=100 stop=94 (6%) → ok '%7 limit içinde'", () => {
      render(<PreTradeChecklist {...HAPPY} entryPrice={100} planStop={94} />);
      expect(screen.getByText(/6\.0% \(Mark %7 limit içinde\)/)).toBeInTheDocument();
    });

    it("entry=100 stop=92 (8%) → fail '%7 mutlak limit AŞILDI'", () => {
      render(<PreTradeChecklist {...HAPPY} entryPrice={100} planStop={92} />);
      expect(screen.getByText(/8\.0%.*%7 mutlak limit AŞILDI/)).toBeInTheDocument();
    });

    it("planStop=null → fail 'Stop \\$ tanımlı değil'", () => {
      render(<PreTradeChecklist {...HAPPY} planStop={null} />);
      expect(screen.getByText(/Stop \$ tanımlı değil/)).toBeInTheDocument();
    });
  });

  describe("R/R hedef hesap", () => {
    it("entry=100 stop=95 target=115 → ok R/R=3.00", () => {
      render(
        <PreTradeChecklist
          {...HAPPY}
          entryPrice={100}
          planStop={95}
          planTarget={115}
        />
      );
      expect(screen.getByText(/R\/R = 3\.00.*Mark uyumlu/)).toBeInTheDocument();
    });

    it("entry=100 stop=95 target=102 → fail R/R=0.40", () => {
      render(
        <PreTradeChecklist
          {...HAPPY}
          entryPrice={100}
          planStop={95}
          planTarget={102}
        />
      );
      expect(screen.getByText(/R\/R = 0\.40.*kabul edilemez/)).toBeInTheDocument();
    });

    it("entry=100 stop=95 target=108 (R/R=1.6) → warn 'zayıf'", () => {
      render(
        <PreTradeChecklist
          {...HAPPY}
          entryPrice={100}
          planStop={95}
          planTarget={108}
        />
      );
      expect(screen.getByText(/R\/R = 1\.60.*zayıf/)).toBeInTheDocument();
    });
  });

  describe("Pozisyon büyüklük (Mark TTLC s.85 sektör limit paralel — P265)", () => {
    it("planSizePct=20 → ok '20%'", () => {
      render(<PreTradeChecklist {...HAPPY} planSizePct={20} />);
      expect(screen.getByText(/20%.*sektör limiti içinde/)).toBeInTheDocument();
    });

    it("planSizePct=27 → warn 'sınır bölgesi'", () => {
      render(<PreTradeChecklist {...HAPPY} planSizePct={27} />);
      expect(screen.getByText(/27%.*sınır bölgesi/)).toBeInTheDocument();
    });

    it("planSizePct=35 → fail 'LİMİT AŞILDI'", () => {
      render(<PreTradeChecklist {...HAPPY} planSizePct={35} />);
      expect(screen.getByText(/35%.*LİMİT AŞILDI/)).toBeInTheDocument();
    });
  });

  describe("Mod farkındalığı (Vizyon İLKE #10)", () => {
    it("defansif → fail 'BLOK'", () => {
      mockTradingMode.mockReturnValue(makeMode("defansif"));
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/DEFANSİF mod.*BLOK/)).toBeInTheDocument();
    });

    it("rehab → warn 'yarım pozisyon'", () => {
      mockTradingMode.mockReturnValue(makeMode("rehab"));
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/REHAB mod.*yarım pozisyon/)).toBeInTheDocument();
    });

    it("agresif → ok 'Conviction High'", () => {
      mockTradingMode.mockReturnValue(makeMode("agresif"));
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/AGRESİF mod.*Conviction High/)).toBeInTheDocument();
    });

    it("normal → ok 'standart %1 R'", () => {
      mockTradingMode.mockReturnValue(makeMode("normal"));
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/NORMAL mod.*standart %1 R/)).toBeInTheDocument();
    });
  });

  describe("Genel sayım (status toplamı = row sayısı)", () => {
    it("HAPPY → tüm 8 koşul ok → '8 / 8 ✓'", () => {
      render(<PreTradeChecklist {...HAPPY} />);
      expect(screen.getByText(/8 \/ 8 ✓/)).toBeInTheDocument();
    });

    it("Header okCount/rows.length göstergesi formatlı", () => {
      render(<PreTradeChecklist {...HAPPY} stage={4} />);
      // Stage 4 fail → 7/8 ok kalır, header "kritik eksik" göstermeli
      expect(screen.getByText(/kritik eksik/)).toBeInTheDocument();
    });
  });
});
