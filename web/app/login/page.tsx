"use client";

import { useState } from "react";

/**
 * P575 (21 Haz 2026): Quanfina giriş sayfası — çerez-tabanlı oturum.
 * Parola /auth/login'e POST → APP_PASSWORD doğrulanır → qf_session çerezi set edilir.
 * Başarılıysa ?next= (veya /) yönlendir. Basic Auth'ın çözemediği "düzgün giriş + çıkış".
 */
export default function LoginPage() {
  const [pw, setPw] = useState("");
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setErr("");
    try {
      const res = await fetch("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: pw }),
      });
      if (res.ok) {
        const params = new URLSearchParams(window.location.search);
        const next = params.get("next") || "/";
        window.location.href = next.startsWith("/") ? next : "/";
      } else {
        setErr("Hatalı parola.");
        setLoading(false);
      }
    } catch {
      setErr("Bağlantı hatası. Tekrar deneyin.");
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <form
        onSubmit={submit}
        className="w-full max-w-sm space-y-6 rounded-xl border border-border bg-card p-8 shadow-sm"
      >
        <div className="space-y-1 text-center">
          <h1 className="text-2xl font-semibold tracking-tight">Quanfina</h1>
          <p className="text-sm text-muted-foreground">Devam etmek için parolanızı girin.</p>
        </div>

        <div className="space-y-2">
          <label htmlFor="pw" className="text-sm font-medium">
            Parola
          </label>
          <input
            id="pw"
            type="password"
            autoFocus
            autoComplete="current-password"
            value={pw}
            onChange={(e) => setPw(e.target.value)}
            className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring"
            placeholder="••••••••"
          />
        </div>

        {err && <p className="text-sm text-red-500">{err}</p>}

        <button
          type="submit"
          disabled={loading || !pw}
          className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground transition hover:opacity-90 disabled:opacity-50"
        >
          {loading ? "Giriş yapılıyor..." : "Giriş"}
        </button>
      </form>
    </div>
  );
}
