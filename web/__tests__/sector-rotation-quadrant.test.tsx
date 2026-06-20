/**
 * SectorRotationQuadrant (P554) — Sektör Rotasyon Kadranı (görsel rotasyon yorumlama).
 *
 * Görece Güç (X=perf_3m−ort) × Momentum (Y=(perf_1m−ort)−(perf_3m−ort)/3). 4 kadran:
 * Lider/Zayıflıyor/Geride/İyileşiyor. <2 geçerli veri → null (MOCK YOK — Kural #28).
 */
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { SectorRotationQuadrant } from "@/components/market/SectorRotationQuadrant";
import type { SectorRotationEntry } from "@/hooks/use-sector-rotation";

function entry(over: Partial<SectorRotationEntry>): SectorRotationEntry {
  return {
    ticker: "XX", sector_name: "S", perf_1w: null, perf_1m: null, perf_3m: null,
    perf_6m: null, perf_1y: null, rs_score: null, rs_rank: null, scan_date: "2026-06-20",
    ...over,
  };
}

// Kontrollü 4 sektör — her biri farklı kadrana düşecek şekilde (mean1m=0.5, mean3m=0):
// LEAD x=24 y=3.5 / WEAK x=18 y=-8.5 / LAG x=-24 y=-4.5 / IMP x=-18 y=9.5
const FOUR: SectorRotationEntry[] = [
  entry({ ticker: "LEAD", sector_name: "Technology", perf_1m: 12, perf_3m: 24, rs_rank: 1 }),
  entry({ ticker: "WEAK", sector_name: "Energy", perf_1m: -2, perf_3m: 18, rs_rank: 2 }),
  entry({ ticker: "LAG", sector_name: "Utilities", perf_1m: -12, perf_3m: -24, rs_rank: 4 }),
  entry({ ticker: "IMP", sector_name: "Materials", perf_1m: 4, perf_3m: -18, rs_rank: 3 }),
];

function titlesByTicker(container: HTMLElement): Record<string, string> {
  const out: Record<string, string> = {};
  container.querySelectorAll("title").forEach((t) => {
    const txt = t.textContent ?? "";
    const m = txt.match(/\(([A-Z]+)\)/);
    if (m) out[m[1]] = txt;
  });
  return out;
}

describe("SectorRotationQuadrant", () => {
  it("4 sektör → her biri doğru kadrana atanır", () => {
    const { container } = render(<SectorRotationQuadrant data={FOUR} />);
    const titles = titlesByTicker(container);
    expect(titles.LEAD).toMatch(/Lider/);
    expect(titles.WEAK).toMatch(/Zayıflıyor/);
    expect(titles.LAG).toMatch(/Geride/);
    expect(titles.IMP).toMatch(/İyileşiyor/);
  });

  it("container + ticker etiketleri render olur", () => {
    const { getByTestId, getAllByText } = render(<SectorRotationQuadrant data={FOUR} />);
    expect(getByTestId("sector-rotation-quadrant")).toBeInTheDocument();
    expect(getAllByText("LEAD").length).toBeGreaterThan(0);
  });

  it("<2 geçerli veri → null (MOCK YOK)", () => {
    const { container } = render(
      <SectorRotationQuadrant data={[entry({ ticker: "A", perf_1m: 5, perf_3m: 5 })]} />
    );
    expect(container.firstChild).toBeNull();
  });

  it("perf_3m/perf_1m null olan sektör grafikten dışlanır", () => {
    const data = [
      ...FOUR,
      entry({ ticker: "NULLP", sector_name: "Real Estate", perf_1m: null, perf_3m: 10 }),
    ];
    const { container } = render(<SectorRotationQuadrant data={data} />);
    const titles = titlesByTicker(container);
    expect(titles.NULLP).toBeUndefined(); // eksik veri → nokta yok
    expect(Object.keys(titles).sort()).toEqual(["IMP", "LAG", "LEAD", "WEAK"]);
  });

  it("metodoloji dipnotu görünür (Kural #26 şeffaflık)", () => {
    const { getByTestId } = render(<SectorRotationQuadrant data={FOUR} />);
    // dipnot <span> çocukları içerir → container textContent ile kontrol
    expect(getByTestId("sector-rotation-quadrant").textContent).toMatch(/sektör ortalaması/i);
  });
});
