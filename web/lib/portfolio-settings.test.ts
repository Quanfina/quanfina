/**
 * P400: portfolio-settings lib testleri (saf localStorage helper).
 *
 * Sn. Ferit'in gerçek portföy büyüklüğü saklı kalsın — paper trading'de
 * pozisyon yüzdesi hesabı doğru çalışsın.
 */
import { describe, it, expect, beforeEach } from "vitest";
import {
  DEFAULT_PORTFOLIO_VALUE,
  PORTFOLIO_VALUE_STORAGE_KEY,
  getPortfolioValue,
  setPortfolioValue,
  clearPortfolioValue,
} from "@/lib/portfolio-settings";

beforeEach(() => {
  window.localStorage.clear();
});

describe("getPortfolioValue", () => {
  it("Bos localStorage -> DEFAULT $100K dondurur", () => {
    expect(getPortfolioValue()).toBe(DEFAULT_PORTFOLIO_VALUE);
    expect(DEFAULT_PORTFOLIO_VALUE).toBe(100000);
  });

  it("Kayitli deger varsa onu dondurur", () => {
    window.localStorage.setItem(PORTFOLIO_VALUE_STORAGE_KEY, "50000");
    expect(getPortfolioValue()).toBe(50000);
  });

  it("Bozuk deger (NaN, negatif, 0) -> default fallback", () => {
    window.localStorage.setItem(PORTFOLIO_VALUE_STORAGE_KEY, "abc");
    expect(getPortfolioValue()).toBe(DEFAULT_PORTFOLIO_VALUE);
    window.localStorage.setItem(PORTFOLIO_VALUE_STORAGE_KEY, "-100");
    expect(getPortfolioValue()).toBe(DEFAULT_PORTFOLIO_VALUE);
    window.localStorage.setItem(PORTFOLIO_VALUE_STORAGE_KEY, "0");
    expect(getPortfolioValue()).toBe(DEFAULT_PORTFOLIO_VALUE);
  });
});

describe("setPortfolioValue", () => {
  it("Gecerli deger kaydedilir + true doner", () => {
    expect(setPortfolioValue(75000)).toBe(true);
    expect(getPortfolioValue()).toBe(75000);
  });

  it("Negatif/sifir/NaN reddedilir (false doner, localStorage degismez)", () => {
    setPortfolioValue(100000);
    expect(setPortfolioValue(-50)).toBe(false);
    expect(setPortfolioValue(0)).toBe(false);
    expect(setPortfolioValue(Number.NaN)).toBe(false);
    expect(setPortfolioValue(Number.POSITIVE_INFINITY)).toBe(false);
    expect(getPortfolioValue()).toBe(100000);  // korundu
  });

  it("Persist senaryosu — kaydedilen tab kapanip acilsa bile gelir", () => {
    setPortfolioValue(250000);
    // Yeni "session" simulasyonu — clear etmeden read et
    expect(getPortfolioValue()).toBe(250000);
  });
});

describe("clearPortfolioValue", () => {
  it("Sifirla -> default'a doner", () => {
    setPortfolioValue(80000);
    expect(getPortfolioValue()).toBe(80000);
    clearPortfolioValue();
    expect(getPortfolioValue()).toBe(DEFAULT_PORTFOLIO_VALUE);
  });
});
