/**
 * CardSection (P545) — /hisse research cockpit bölüm sarmalayıcı.
 * Başlık (h2) + çocukları responsive grid içinde render.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { CardSection } from "@/components/stock/CardSection";

describe("CardSection (P545)", () => {
  it("başlık + çocukları render eder", () => {
    render(
      <CardSection title="Carr Setupları">
        <div data-testid="child-a">A</div>
        <div data-testid="child-b">B</div>
      </CardSection>,
    );
    expect(screen.getByText("Carr Setupları")).toBeInTheDocument();
    expect(screen.getByTestId("child-a")).toBeInTheDocument();
    expect(screen.getByTestId("child-b")).toBeInTheDocument();
  });

  it("data-testid bölüm başlığını içerir", () => {
    render(<CardSection title="Karar & Risk"><span>x</span></CardSection>);
    expect(screen.getByTestId("card-section-Karar & Risk")).toBeInTheDocument();
  });
});
