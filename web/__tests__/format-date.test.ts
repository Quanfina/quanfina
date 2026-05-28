/**
 * formatDateTR (KARAR #471 — 20 May 2026).
 *
 * ISO 8601 / YYYY-MM-DD → DD.MM.YYYY TR format. 3+ sayfa kullanan
 * DRY helper (Bilgi Mimarisi İlke #4 tek doğruluk kaynağı).
 */
import { describe, it, expect } from "vitest";
import { formatDateTR } from "@/lib/format-date";

describe("formatDateTR — ISO → DD.MM.YYYY", () => {
  describe("Pure date (saat yok)", () => {
    it.each<[string, string]>([
      ["2026-05-28", "28.05.2026"],
      ["2026-01-01", "01.01.2026"],
      ["1999-12-31", "31.12.1999"],
      ["2020-02-29", "29.02.2020"], // artık yıl
    ])("formatDateTR('%s') → '%s'", (input, expected) => {
      expect(formatDateTR(input)).toBe(expected);
    });
  });

  describe("Date + saat (YYYY-MM-DD HH:MM)", () => {
    it("'2026-05-28 14:30' → '28.05.2026 14:30'", () => {
      expect(formatDateTR("2026-05-28 14:30")).toBe("28.05.2026 14:30");
    });

    it("'2026-05-28 09:00:15' → '28.05.2026 09:00:15'", () => {
      expect(formatDateTR("2026-05-28 09:00:15")).toBe("28.05.2026 09:00:15");
    });
  });

  describe("ISO 8601 'T' ayırıcı", () => {
    it("'2026-05-28T14:30:00' → '28.05.2026 14:30:00' (T → boşluk)", () => {
      expect(formatDateTR("2026-05-28T14:30:00")).toBe("28.05.2026 14:30:00");
    });

    it("'2026-05-28T14:30:00Z' → 'Z' kırpılır", () => {
      expect(formatDateTR("2026-05-28T14:30:00Z")).toBe("28.05.2026 14:30:00");
    });
  });

  describe("Boş/null/undefined → '—'", () => {
    it("null → '—'", () => {
      expect(formatDateTR(null)).toBe("—");
    });

    it("undefined → '—'", () => {
      expect(formatDateTR(undefined)).toBe("—");
    });

    it("'' (boş string) → '—'", () => {
      expect(formatDateTR("")).toBe("—");
    });
  });

  describe("Defensive: parse edilemeyen → ham değer geri döner", () => {
    it("'28/05/2026' → '28/05/2026' (regex eşleşmez)", () => {
      expect(formatDateTR("28/05/2026")).toBe("28/05/2026");
    });

    it("'invalid' → 'invalid' (defansif)", () => {
      expect(formatDateTR("invalid")).toBe("invalid");
    });

    it("'2026-5-28' (zero-pad yok) → 'invalid' regex eşleşmez, ham döner", () => {
      expect(formatDateTR("2026-5-28")).toBe("2026-5-28");
    });
  });
});
