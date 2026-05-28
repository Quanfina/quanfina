"use client";

import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "@/components/layout/theme-provider";
import { TooltipProvider } from "@/components/ui/tooltip";
// NOT (Paket 355): AG Grid modül kaydı buradan kaldırıldı. Global provider
// olduğu için ag-grid (~500KB) tüm route'ların paylaşılan chunk'ına giriyordu.
// Kayıt artık grid kullanan sayfalarda: `import "@/lib/ag-grid-setup"`.

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 10_000,
            retry: 1,
            // Kod kalitesi incelemesi (28 May 2026): pencere odağında TÜM aktif
            // sorgular yeniden fetch ediliyordu (React Query default true) →
            // gereksiz yfinance kota + Cloud SQL yükü. Veriler zaten staleTime +
            // refetchInterval (canlı fiyat) ile yönetiliyor; odak-fetch gereksiz.
            refetchOnWindowFocus: false,
          },
        },
      })
  );

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider delay={400}>{children}</TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
