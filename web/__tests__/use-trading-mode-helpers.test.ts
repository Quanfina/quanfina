import { describe, it, expect } from "vitest";
import {
  getDefansifBlockMessage,
  isNewAlBlocked,
  getModUiTheme,
  type TradingMode,
} from "@/hooks/use-trading-mode";

describe("useTradingMode DRY helpers (P233, P234, P259)", () => {
  describe("getDefansifBlockMessage", () => {
    it("defansif modda Mark TTLC s.187 mesajı dondurur", () => {
      const msg = getDefansifBlockMessage("defansif");
      expect(msg).not.toBeNull();
      expect(msg).toContain("DEFANSİF");
      expect(msg).toContain("Mark TTLC s.187");
    });

    it.each<TradingMode>(["normal", "rehab", "agresif"])(
      "%s modda null doner",
      (mode) => {
        expect(getDefansifBlockMessage(mode)).toBeNull();
      }
    );
  });

  describe("isNewAlBlocked", () => {
    it("defansif modda yeni AL bloklu", () => {
      expect(isNewAlBlocked("defansif")).toBe(true);
    });

    it.each<TradingMode>(["normal", "rehab", "agresif"])(
      "%s modda yeni AL aktif",
      (mode) => {
        expect(isNewAlBlocked(mode)).toBe(false);
      }
    );
  });

  describe("getModUiTheme", () => {
    it("normal modda null doner (banner gizli)", () => {
      expect(getModUiTheme("normal")).toBeNull();
    });

    it("defansif tema: kalkan emoji + danger renk", () => {
      const theme = getModUiTheme("defansif");
      expect(theme).not.toBeNull();
      expect(theme?.emoji).toBe("🛡️");
      expect(theme?.color).toBe("var(--mtp-danger)");
      expect(theme?.shortMessage).toContain("DEFANSİF");
    });

    it("rehab tema: bandaj emoji + amber renk", () => {
      const theme = getModUiTheme("rehab");
      expect(theme).not.toBeNull();
      expect(theme?.emoji).toBe("🩹");
      expect(theme?.color).toBe("#F59E0B");
      expect(theme?.shortMessage).toContain("REHAB");
    });

    it("agresif tema: roket emoji + excellent renk", () => {
      const theme = getModUiTheme("agresif");
      expect(theme).not.toBeNull();
      expect(theme?.emoji).toBe("🚀");
      expect(theme?.color).toBe("var(--mtp-excellent)");
      expect(theme?.shortMessage).toContain("AGRESİF");
    });

    it("3 non-null tema da background + borderColor donerir", () => {
      for (const mode of ["defansif", "rehab", "agresif"] as TradingMode[]) {
        const theme = getModUiTheme(mode);
        expect(theme?.background).toBeTruthy();
        expect(theme?.borderColor).toBeTruthy();
      }
    });
  });
});
