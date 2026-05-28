/**
 * Error boundary fallback (Paket 354) — dashboard + global error.tsx.
 *
 * Next.js 16 error boundary Client Component'leri. Boundary'nin Next.js
 * mekanizması framework-test edilir; burada fallback UI + unstable_retry
 * wiring'i test edilir (objektif ayna dil + retry butonu).
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import DashboardError from "@/app/(dashboard)/error";
import GlobalError from "@/app/global-error";

describe("DashboardError — fallback UI", () => {
  it("hata mesajı + 'Tekrar dene' butonu render", () => {
    render(<DashboardError error={new Error("boom")} unstable_retry={vi.fn()} />);
    expect(screen.getByText("Bu sayfa yüklenemedi")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Tekrar dene/ })).toBeInTheDocument();
  });

  it("menü canlı kalır mesajı (objektif ayna dil — his/övgü yok)", () => {
    render(<DashboardError error={new Error("x")} unstable_retry={vi.fn()} />);
    const text = document.body.textContent ?? "";
    expect(text).toMatch(/Menü ve diğer sayfalar çalışmaya/);
    // Yağcılık/his dili olmamalı (KALICI İLKE #11)
    expect(text).not.toMatch(/üzülme|aferin|merak etme/i);
  });

  it("'Tekrar dene' → unstable_retry çağrılır", () => {
    const retry = vi.fn();
    render(<DashboardError error={new Error("x")} unstable_retry={retry} />);
    fireEvent.click(screen.getByRole("button", { name: /Tekrar dene/ }));
    expect(retry).toHaveBeenCalledTimes(1);
  });

  it("error.digest varsa hata kimliği gösterilir", () => {
    const err = Object.assign(new Error("x"), { digest: "abc123" });
    render(<DashboardError error={err} unstable_retry={vi.fn()} />);
    expect(screen.getByText(/abc123/)).toBeInTheDocument();
  });

  it("error.digest yoksa kimlik satırı render edilmez", () => {
    render(<DashboardError error={new Error("x")} unstable_retry={vi.fn()} />);
    expect(screen.queryByText(/Hata kimliği/)).not.toBeInTheDocument();
  });
});

describe("GlobalError — root fallback", () => {
  it("kök hata mesajı + retry butonu render", () => {
    render(<GlobalError error={new Error("root boom")} unstable_retry={vi.fn()} />);
    expect(screen.getByText("Uygulama başlatılamadı")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Tekrar dene/ })).toBeInTheDocument();
  });

  it("retry butonu unstable_retry'i çağırır", () => {
    const retry = vi.fn();
    render(<GlobalError error={new Error("x")} unstable_retry={retry} />);
    fireEvent.click(screen.getByRole("button", { name: /Tekrar dene/ }));
    expect(retry).toHaveBeenCalledTimes(1);
  });
});
