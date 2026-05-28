"use client"; // Error boundary Client Component olmalı (Next.js 16 zorunlu)

/**
 * Dashboard route-level error boundary (Paket 354 — sağlamlaştırma).
 *
 * Önceden hiç error boundary yoktu: tek bir component render'da throw ederse
 * TÜM uygulama beyaz ekrana düşüyordu. Bu boundary sayfa segmentini sarar —
 * hata olursa Sidebar + menü canlı kalır, sadece içerik fallback gösterir.
 *
 * Objektif ayna dil (KALICI İLKE #11): durum + aksiyon, his/övgü yok.
 */
import { useEffect } from "react";
import { AlertTriangle } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function DashboardError({
  error,
  unstable_retry,
}: {
  error: Error & { digest?: string };
  unstable_retry: () => void;
}) {
  useEffect(() => {
    // Hata izleme — şimdilik console (ileride error reporting servisine bağlanabilir)
    console.error("[dashboard] render hatası:", error);
  }, [error]);

  return (
    <div className="flex flex-1 items-center justify-center p-8">
      <div className="w-full max-w-md rounded-lg border bg-card p-6 flex flex-col items-center gap-4 text-center">
        <AlertTriangle size={32} style={{ color: "var(--mtp-danger, #dc3545)" }} />
        <div className="flex flex-col gap-1">
          <h2 className="text-lg font-semibold">Bu sayfa yüklenemedi</h2>
          <p className="text-sm text-muted-foreground">
            Beklenmeyen bir render hatası oluştu. Menü ve diğer sayfalar çalışmaya
            devam ediyor.
          </p>
          {error.digest && (
            <p className="text-xs text-muted-foreground font-mono mt-1">
              Hata kimliği: {error.digest}
            </p>
          )}
        </div>
        <Button onClick={() => unstable_retry()}>Tekrar dene</Button>
      </div>
    </div>
  );
}
