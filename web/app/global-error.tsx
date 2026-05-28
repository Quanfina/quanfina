"use client"; // Error boundary Client Component olmalı (Next.js 16 zorunlu)

/**
 * Root global error boundary (Paket 354 — sağlamlaştırma).
 *
 * Kök layout'un kendisi throw ederse devreye girer (son çare). global-error
 * kök layout'u DEĞİŞTİRİR — kendi <html>/<body> ve stilini içermeli (Tailwind
 * global stilleri yüklenmemiş olabilir → inline style). Karanlık tema default.
 *
 * Objektif ayna dil (KALICI İLKE #11): durum + aksiyon.
 */
import { useEffect } from "react";

export default function GlobalError({
  error,
  unstable_retry,
}: {
  error: Error & { digest?: string };
  unstable_retry: () => void;
}) {
  useEffect(() => {
    console.error("[global] kök layout hatası:", error);
  }, [error]);

  return (
    <html lang="tr">
      <body style={{ margin: 0, fontFamily: "system-ui, -apple-system, sans-serif" }}>
        <div
          style={{
            minHeight: "100vh",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "2rem",
            background: "#0a0a0a",
            color: "#fafafa",
          }}
        >
          <div style={{ maxWidth: "28rem", textAlign: "center" }}>
            <h2 style={{ fontSize: "1.25rem", fontWeight: 600, margin: 0 }}>
              Uygulama başlatılamadı
            </h2>
            <p style={{ fontSize: "0.875rem", opacity: 0.7, marginTop: "0.5rem" }}>
              Kök seviyede beklenmeyen bir hata oluştu.
              {error.digest ? ` (Hata kimliği: ${error.digest})` : ""}
            </p>
            <button
              onClick={() => unstable_retry()}
              style={{
                marginTop: "1.25rem",
                padding: "0.5rem 1.25rem",
                borderRadius: "0.375rem",
                border: "1px solid #3a3a3a",
                background: "#1a1a1a",
                color: "#fafafa",
                cursor: "pointer",
                fontSize: "0.875rem",
              }}
            >
              Tekrar dene
            </button>
          </div>
        </div>
      </body>
    </html>
  );
}
