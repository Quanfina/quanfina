/**
 * watchlist-status (KARAR #469) + passed-signals (KARAR #475 — UX Bölüm 6).
 *
 * Watchlist hiyerarşi: watch → on_deck → focus → buy. Promote/demote disiplini.
 * Geçilen sinyaller: Sn. Ferit "GEÇ" tıklayınca localStorage'da kalıcı.
 */
import { describe, it, expect, beforeEach } from "vitest";
import {
  STATUS_HIERARCHY,
  promoteStatus,
  demoteStatus,
  canPromote,
  canDemote,
} from "@/lib/watchlist-status";
import {
  getPassedSignals,
  setPassedSignals,
  signalKey,
} from "@/lib/passed-signals";

describe("watchlist-status — 4 katman hiyerarşi", () => {
  it("STATUS_HIERARCHY = ['watch', 'on_deck', 'focus', 'buy']", () => {
    expect(STATUS_HIERARCHY).toEqual(["watch", "on_deck", "focus", "buy"]);
  });

  describe("promoteStatus — yukarı çıkış", () => {
    it.each<[string, string]>([
      ["watch", "on_deck"],
      ["on_deck", "focus"],
      ["focus", "buy"],
    ])("'%s' → '%s'", (current, expected) => {
      expect(promoteStatus(current)).toBe(expected);
    });

    it("'buy' (tepe) → 'buy' (zaten en üst)", () => {
      expect(promoteStatus("buy")).toBe("buy");
    });

    it("Bilinmeyen status → kendisi (idx=-1)", () => {
      expect(promoteStatus("invalid")).toBe("invalid");
    });
  });

  describe("demoteStatus — aşağı düşüş", () => {
    it.each<[string, string]>([
      ["buy", "focus"],
      ["focus", "on_deck"],
      ["on_deck", "watch"],
    ])("'%s' → '%s'", (current, expected) => {
      expect(demoteStatus(current)).toBe(expected);
    });

    it("'watch' (zemin) → 'watch'", () => {
      expect(demoteStatus("watch")).toBe("watch");
    });

    it("Bilinmeyen status → kendisi (idx=-1)", () => {
      expect(demoteStatus("invalid")).toBe("invalid");
    });
  });

  describe("canPromote / canDemote — sınır kontrol", () => {
    it("canPromote: 'buy' hariç hepsi true", () => {
      expect(canPromote("watch")).toBe(true);
      expect(canPromote("on_deck")).toBe(true);
      expect(canPromote("focus")).toBe(true);
      expect(canPromote("buy")).toBe(false);
    });

    it("canDemote: 'watch' hariç hepsi true", () => {
      expect(canDemote("watch")).toBe(false);
      expect(canDemote("on_deck")).toBe(true);
      expect(canDemote("focus")).toBe(true);
      expect(canDemote("buy")).toBe(true);
    });
  });
});

describe("passed-signals — Sn. Ferit 'GEÇ' disiplini (KARAR #475)", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("Boş localStorage → boş Set", () => {
    expect(getPassedSignals().size).toBe(0);
  });

  it("set + get round-trip", () => {
    const set = new Set(["AAPL-minervini", "NVDA-carr"]);
    setPassedSignals(set);
    const fetched = getPassedSignals();
    expect(fetched.size).toBe(2);
    expect(fetched.has("AAPL-minervini")).toBe(true);
    expect(fetched.has("NVDA-carr")).toBe(true);
  });

  it("Geçersiz JSON → boş Set (defansif)", () => {
    localStorage.setItem("quanfina_passed_signals", "not-json{");
    expect(getPassedSignals().size).toBe(0);
  });

  it("Array olmayan JSON → boş Set (defansif)", () => {
    localStorage.setItem("quanfina_passed_signals", '{"not": "array"}');
    expect(getPassedSignals().size).toBe(0);
  });

  it("Boş Set save → '[]' depolanır, get sonrası yine boş", () => {
    setPassedSignals(new Set());
    expect(localStorage.getItem("quanfina_passed_signals")).toBe("[]");
    expect(getPassedSignals().size).toBe(0);
  });

  describe("signalKey — symbol+strategy unique key (KARAR #469)", () => {
    it("'AAPL' + 'minervini' → 'AAPL-minervini'", () => {
      expect(signalKey("AAPL", "minervini")).toBe("AAPL-minervini");
    });

    it("'NVDA' + 'carr' → 'NVDA-carr'", () => {
      expect(signalKey("NVDA", "carr")).toBe("NVDA-carr");
    });

    it("Format konsistans: aynı (symbol, strategy) → aynı key", () => {
      const k1 = signalKey("MSFT", "minervini");
      const k2 = signalKey("MSFT", "minervini");
      expect(k1).toBe(k2);
    });

    it("Symbol case-sensitive (büyük harf disiplini caller sorumluluğu)", () => {
      expect(signalKey("aapl", "minervini")).toBe("aapl-minervini");
      expect(signalKey("AAPL", "minervini")).not.toBe(signalKey("aapl", "minervini"));
    });
  });
});
