"use client";

import { useState } from "react";
import { LogOut } from "lucide-react";

/**
 * P575 (21 Haz 2026): Çıkış butonu — POST /auth/logout → qf_session çerezi silinir →
 * /login'e yönlendir. Basic Auth'ın çözemediği temiz logout (sidebar footer).
 */
export function LogoutButton() {
  const [loading, setLoading] = useState(false);

  async function logout() {
    setLoading(true);
    try {
      await fetch("/auth/logout", { method: "POST" });
    } catch {
      // çerez silme isteği hata verse bile /login'e gidiyoruz
    }
    window.location.href = "/login";
  }

  return (
    <button
      type="button"
      onClick={logout}
      disabled={loading}
      className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
    >
      <LogOut size={15} strokeWidth={1.75} />
      {loading ? "Çıkış yapılıyor..." : "Çıkış"}
    </button>
  );
}
