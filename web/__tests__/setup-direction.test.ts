/**
 * setupDirection (P558) — Sinyaller YÖN sütunu yön türetme.
 *
 * Carr SHORT setupları (blue_sea/gap_down/rising_wedge — backend P518/P520/P522 bulk SHORT
 * birebir) → SHORT; diğer her setup (Minervini + Carr LONG) + null → LONG.
 */
import { describe, it, expect } from "vitest";
import { setupDirection, SHORT_SETUPS } from "@/types/trade";

describe("setupDirection (Sinyaller YÖN)", () => {
  it("Carr SHORT setupları (slug formu) → SHORT", () => {
    expect(setupDirection("blue_sea")).toBe("SHORT");
    expect(setupDirection("gap_down")).toBe("SHORT");
    expect(setupDirection("rising_wedge")).toBe("SHORT");
  });

  it("Carr SHORT setupları (LABEL formu — /api/signals karışık veri) → SHORT", () => {
    // Gerçek-veri doğrulaması: setup_type bazı kayıtlarda label gelir. Bunlar da SHORT olmalı,
    // yoksa label-formlu SHORT sinyal yanlışlıkla LONG görünür (P558 bug-fix).
    expect(setupDirection("Blue Sea Breakdown")).toBe("SHORT");
    expect(setupDirection("Gap Down")).toBe("SHORT");
    expect(setupDirection("Rising Wedge")).toBe("SHORT");
    expect(setupDirection("  rising wedge  ")).toBe("SHORT"); // trim + case-insensitive
  });

  it("Minervini + Carr LONG setupları (slug) → LONG", () => {
    for (const s of ["vcp", "pivot", "pocket_pivot", "power_play", "pullback",
                     "mean_reversion", "blue_sky", "bullish_base", "bullish_divergence"]) {
      expect(setupDirection(s)).toBe("LONG");
    }
  });

  it("LABEL-form LONG (blue_sky vs blue_sea karışıklığı guard) → LONG", () => {
    // "Blue Sky Breakout" (LONG) ≠ "Blue Sea Breakdown" (SHORT) — tehlikeli benzerlik.
    expect(setupDirection("Blue Sky Breakout")).toBe("LONG");
    expect(setupDirection("Bullish Divergence")).toBe("LONG");
    expect(setupDirection("Coiled Spring")).toBe("LONG");
    expect(setupDirection("VCP")).toBe("LONG");
  });

  it("null / undefined / bilinmeyen → LONG (geriye uyum)", () => {
    expect(setupDirection(null)).toBe("LONG");
    expect(setupDirection(undefined)).toBe("LONG");
    expect(setupDirection("bilinmeyen_setup")).toBe("LONG");
    expect(setupDirection("")).toBe("LONG");
  });

  it("SHORT_SETUPS tam 3 kanonik Carr SHORT (uydurma yok — Kural #26)", () => {
    expect(SHORT_SETUPS.size).toBe(3);
    expect([...SHORT_SETUPS].sort()).toEqual(["blue_sea", "gap_down", "rising_wedge"]);
  });
});
