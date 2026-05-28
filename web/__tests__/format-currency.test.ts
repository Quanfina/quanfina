/**
 * fmtUsd + fmtUsdCompact + fmtPctSigned (KARAR #733 alt-paket 43).
 *
 * 9+ sayfa kullanan DRY format helper'ları (Bilgi Mimarisi İlke #4).
 */
import { describe, it, expect } from "vitest";
import {
  fmtUsd,
  fmtUsdCompact,
  fmtPctSigned,
} from "@/lib/format-currency";

describe("fmtUsd — ABD locale dolar format", () => {
  it.each<[number, string]>([
    [100_000, "$100,000.00"],
    [1450.30, "$1,450.30"],
    [0, "$0.00"],
    [1, "$1.00"],
    [1_000_000.99, "$1,000,000.99"],
    [-500.5, "$-500.50"],  // toLocaleString negatif + $ prefix → "$-500.50"
  ])("fmtUsd(%s) → %s", (input, expected) => {
    expect(fmtUsd(input)).toBe(expected);
  });

  it("decimals=4 → 4 ondalık basamak", () => {
    expect(fmtUsd(0.5, 4)).toBe("$0.5000");
  });

  it("decimals=0 → tam sayı (no fraction)", () => {
    expect(fmtUsd(1234, 0)).toBe("$1,234");
  });

  it("null → '—'", () => {
    expect(fmtUsd(null)).toBe("—");
  });

  it("undefined → '—'", () => {
    expect(fmtUsd(undefined)).toBe("—");
  });

  it("NaN → '—'", () => {
    expect(fmtUsd(NaN)).toBe("—");
  });
});

describe("fmtUsdCompact — K/M kısa format", () => {
  it.each<[number, string]>([
    [1500, "$1.5K"],
    [9999, "$10.0K"],
    [2_500_000, "$2.5M"],
    [1_000_000, "$1.0M"],
    [999, "$999.00"],          // 1K altı → tam fmtUsd
    [-1500, "$-1.5K"],         // Negatif K
    [-2_500_000, "$-2.5M"],    // Negatif M
  ])("fmtUsdCompact(%s) → %s", (input, expected) => {
    expect(fmtUsdCompact(input)).toBe(expected);
  });

  it("null → '—'", () => {
    expect(fmtUsdCompact(null)).toBe("—");
  });

  it("0 → '$0.00' (1K altı fmtUsd'a düşer)", () => {
    expect(fmtUsdCompact(0)).toBe("$0.00");
  });
});

describe("fmtPctSigned — işaretli yüzde", () => {
  it.each<[number, string]>([
    [5.2, "+5.20%"],
    [0, "0.00%"],              // 0 işaretsiz
    [100, "+100.00%"],
    [-3.1, "-3.10%"],
    [-0.5, "-0.50%"],
  ])("fmtPctSigned(%s) → %s", (input, expected) => {
    expect(fmtPctSigned(input)).toBe(expected);
  });

  it("decimals=1 → tek ondalık", () => {
    expect(fmtPctSigned(-3.157, 1)).toBe("-3.2%");
  });

  it("decimals=0 → tam sayı", () => {
    expect(fmtPctSigned(5.7, 0)).toBe("+6%");
  });

  it("null/undefined/NaN → '—'", () => {
    expect(fmtPctSigned(null)).toBe("—");
    expect(fmtPctSigned(undefined)).toBe("—");
    expect(fmtPctSigned(NaN)).toBe("—");
  });
});
