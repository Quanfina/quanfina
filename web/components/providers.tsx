"use client";

import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "@/components/layout/theme-provider";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";

ModuleRegistry.registerModules([AllCommunityModule]);

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
