/**
 * P541 — Journal YÖN kolonu (LONG/SHORT). invest_type 1=LONG (yeşil) / 2=SHORT (kırmızı).
 * Eski trade'ler (invest_type yok) → LONG (geriye uyum).
 */
import { describe, it, expect } from "vitest";
import { TRADE_COL_DEFS } from "@/components/journal/columns";

const col = TRADE_COL_DEFS.find((c) => (c as { field?: string }).field === "invest_type") as {
  headerName: string;
  valueFormatter: (p: { value: unknown }) => string;
  cellStyle: (p: { value: unknown }) => { color: string };
};

describe("Journal YÖN kolonu (P541)", () => {
  it("kolon mevcut + başlık 'YÖN'", () => {
    expect(col).toBeTruthy();
    expect(col.headerName).toBe("YÖN");
  });

  it("invest_type=2 → 'SHORT' + danger rengi", () => {
    expect(col.valueFormatter({ value: 2 })).toBe("SHORT");
    expect(col.cellStyle({ value: 2 }).color).toBe("var(--mtp-danger)");
  });

  it("invest_type=1 → 'LONG' + excellent rengi", () => {
    expect(col.valueFormatter({ value: 1 })).toBe("LONG");
    expect(col.cellStyle({ value: 1 }).color).toBe("var(--mtp-excellent)");
  });

  it("invest_type yok (eski trade) → 'LONG' (geriye uyum)", () => {
    expect(col.valueFormatter({ value: undefined })).toBe("LONG");
    expect(col.cellStyle({ value: undefined }).color).toBe("var(--mtp-excellent)");
  });
});
