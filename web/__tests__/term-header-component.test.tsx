/**
 * P428 (31 May 2026 — BUG FIX): TermHeaderComponent tıklayarak sıralama testi.
 * Önceki sürüm custom headerComponent AG Grid sort-on-click'i eziyordu;
 * progressSort çağrılmıyordu. Bu test: etiket tıklanınca progressSort,
 * (?) tooltip butonu ayrı (sort tetiklemez), sıralama oku render.
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";

// useTerms mock (TermTooltip içinde) — boş terim
vi.mock("@/hooks/use-terms", () => ({
  useTerms: () => ({ data: [] }),
}));

type SortState = "asc" | "desc" | null;

function makeProps(over: Partial<Record<string, unknown>> = {}) {
  let sortVal: SortState = (over.sort as SortState) ?? null;
  const listeners: Record<string, (() => void)[]> = {};
  return {
    displayName: "RS",
    termKey: "rs_ibd",
    enableSorting: over.enableSorting ?? true,
    progressSort: over.progressSort ?? vi.fn(),
    column: {
      getSort: () => sortVal,
      addEventListener: (ev: string, cb: () => void) => {
        (listeners[ev] ??= []).push(cb);
      },
      removeEventListener: vi.fn(),
    },
    // test helper: sortChanged tetikle
    _setSort: (v: SortState) => {
      sortVal = v;
      (listeners["sortChanged"] ?? []).forEach((cb) => cb());
    },
  } as never;
}

describe("TermHeaderComponent — sıralama (P428 fix)", () => {
  it("başlık etiketine tıklayınca progressSort çağrılır", () => {
    const progressSort = vi.fn();
    render(<TermHeaderComponent {...makeProps({ progressSort })} />);
    fireEvent.click(screen.getByTestId("term-header-sort"));
    expect(progressSort).toHaveBeenCalledTimes(1);
  });

  it("shift+click multi-sort (progressSort(true))", () => {
    const progressSort = vi.fn();
    render(<TermHeaderComponent {...makeProps({ progressSort })} />);
    fireEvent.click(screen.getByTestId("term-header-sort"), { shiftKey: true });
    expect(progressSort).toHaveBeenCalledWith(true);
  });

  it("enableSorting=false → tıklama progressSort çağırmaz", () => {
    const progressSort = vi.fn();
    render(<TermHeaderComponent {...makeProps({ progressSort, enableSorting: false })} />);
    fireEvent.click(screen.getByTestId("term-header-sort"));
    expect(progressSort).not.toHaveBeenCalled();
  });

  it("displayName render edilir", () => {
    render(<TermHeaderComponent {...makeProps()} />);
    expect(screen.getByText("RS")).toBeInTheDocument();
  });

  it("(?) tooltip butonu var (term-header-sort'tan ayrı)", () => {
    render(<TermHeaderComponent {...makeProps()} />);
    // TermTooltip (?) butonu aria-label ile
    expect(screen.getByLabelText(/rs_ibd terimini açıkla/)).toBeInTheDocument();
  });
});
