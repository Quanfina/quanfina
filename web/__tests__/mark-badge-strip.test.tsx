/**
 * MarkBadgeStrip (KARAR ADAY #721) — Mark canon rozet generic komponent.
 *
 * Props-based conditional badge: vcp_quality / power_play / ready / tennis_ball /
 * volume_asymmetry / carr_stage / code_33. density compact vs full ayrımı.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";

describe("MarkBadgeStrip — boş durum", () => {
  it("Boş signals + showEmpty=false → null (hiçbir şey render etmez)", () => {
    const { container } = render(<MarkBadgeStrip signals={{}} />);
    expect(container.firstChild).toBeNull();
  });

  it("Boş signals + showEmpty=true → '—' placeholder", () => {
    render(<MarkBadgeStrip signals={{}} showEmpty />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

describe("MarkBadgeStrip — VCP Quality rozetleri", () => {
  it("vcp_quality_score='EXCELLENT' → 🏆 VCP A+", () => {
    render(<MarkBadgeStrip signals={{ vcp_quality_score: "EXCELLENT" }} />);
    expect(screen.getByText("VCP A+")).toBeInTheDocument();
  });

  it("vcp_quality_score='PASS' + density='full' → 📊 VCP Pass", () => {
    render(
      <MarkBadgeStrip
        signals={{ vcp_quality_score: "PASS" }}
        density="full"
      />
    );
    expect(screen.getByText("VCP Pass")).toBeInTheDocument();
  });

  it("vcp_quality_score='PASS' + density='compact' (default) → VCP Pass GİZLİ", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ vcp_quality_score: "PASS" }} />
    );
    expect(screen.queryByText("VCP Pass")).not.toBeInTheDocument();
    expect(container.firstChild).toBeNull(); // tek PASS compact → boş
  });
});

describe("MarkBadgeStrip — Power Play + Ready + Tennis Ball", () => {
  it("power_play_pass=true → ⚡ Power Play", () => {
    render(<MarkBadgeStrip signals={{ power_play_pass: true }} />);
    expect(screen.getByText("Power Play")).toBeInTheDocument();
  });

  it("power_play_pass=false → Power Play YOK", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ power_play_pass: false }} />
    );
    expect(container.firstChild).toBeNull();
  });

  it("vcp_ready_score=85 (≥70) → 🎯 Ready 85", () => {
    render(<MarkBadgeStrip signals={{ vcp_ready_score: 85 }} />);
    expect(screen.getByText("Ready 85")).toBeInTheDocument();
  });

  it("vcp_ready_score=65 (<70) → Ready GİZLİ (eşik altı)", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ vcp_ready_score: 65 }} />
    );
    expect(container.firstChild).toBeNull();
  });

  it("tennis_ball_pattern='TENNIS_BALL' → 🎾 Tennis Ball", () => {
    render(<MarkBadgeStrip signals={{ tennis_ball_pattern: "TENNIS_BALL" }} />);
    expect(screen.getByText("Tennis Ball")).toBeInTheDocument();
  });

  it("tennis_ball_pattern='partial' → Tennis Ball YOK", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ tennis_ball_pattern: "partial" }} />
    );
    expect(container.firstChild).toBeNull();
  });
});

describe("MarkBadgeStrip — Volume Asymmetry", () => {
  it("volume_asymmetry_tier='healthy' → 📈 Healthy Accum", () => {
    render(<MarkBadgeStrip signals={{ volume_asymmetry_tier: "healthy" }} />);
    expect(screen.getByText("Healthy Accum")).toBeInTheDocument();
  });

  it("volume_asymmetry_tier='distribution' + full → 📉 Distribution", () => {
    render(
      <MarkBadgeStrip
        signals={{ volume_asymmetry_tier: "distribution" }}
        density="full"
      />
    );
    expect(screen.getByText("Distribution")).toBeInTheDocument();
  });

  it("volume_asymmetry_tier='distribution' + compact → GİZLİ", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ volume_asymmetry_tier: "distribution" }} />
    );
    expect(container.firstChild).toBeNull();
  });
});

describe("MarkBadgeStrip — Carr Stage rozetleri (KARAR #733)", () => {
  it("carr_stage=2 → 📈 Stage 2 (Advancing, compact'ta görünür)", () => {
    render(<MarkBadgeStrip signals={{ carr_stage: 2 }} />);
    expect(screen.getByText("Stage 2")).toBeInTheDocument();
  });

  it("carr_stage=4 → ⛔ Stage 4 (Declining, compact'ta görünür — uzak dur)", () => {
    render(<MarkBadgeStrip signals={{ carr_stage: 4 }} />);
    expect(screen.getByText("Stage 4")).toBeInTheDocument();
  });

  it("carr_stage=1 + compact → GİZLİ (sadece full)", () => {
    const { container } = render(<MarkBadgeStrip signals={{ carr_stage: 1 }} />);
    expect(container.firstChild).toBeNull();
  });

  it("carr_stage=1 + full → ⏳ Stage 1 (Basing)", () => {
    render(<MarkBadgeStrip signals={{ carr_stage: 1 }} density="full" />);
    expect(screen.getByText("Stage 1")).toBeInTheDocument();
  });

  it("carr_stage=3 + full → ⚠️ Stage 3 (Topping)", () => {
    render(<MarkBadgeStrip signals={{ carr_stage: 3 }} density="full" />);
    expect(screen.getByText("Stage 3")).toBeInTheDocument();
  });
});

describe("MarkBadgeStrip — Code 33 + çoklu rozet", () => {
  it("code_33_pattern='CODE_33' → ⭐ Code 33", () => {
    render(<MarkBadgeStrip signals={{ code_33_pattern: "CODE_33" }} />);
    expect(screen.getByText("Code 33")).toBeInTheDocument();
  });

  it("Çoklu sinyal (VCP A+ + Power Play + Stage 2) → 3 rozet birlikte", () => {
    render(
      <MarkBadgeStrip
        signals={{
          vcp_quality_score: "EXCELLENT",
          power_play_pass: true,
          carr_stage: 2,
        }}
      />
    );
    expect(screen.getByText("VCP A+")).toBeInTheDocument();
    expect(screen.getByText("Power Play")).toBeInTheDocument();
    expect(screen.getByText("Stage 2")).toBeInTheDocument();
  });

  it("Tooltip (title attr) Mark kaynak atfı içerir (KALICI İLKE #4)", () => {
    const { container } = render(
      <MarkBadgeStrip signals={{ vcp_quality_score: "EXCELLENT" }} />
    );
    const badge = container.querySelector("span[title]");
    expect(badge?.getAttribute("title")).toMatch(/TLSMW|KARAR #466/);
  });
});
