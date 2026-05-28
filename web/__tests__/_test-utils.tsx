/**
 * Vitest test util'ları — React Query hook'ları için QueryClient wrapper.
 *
 * Kullanım:
 *   const { result } = renderHook(() => useFoo(), { wrapper: withQueryClient() });
 *
 * Disiplin: her test izole QueryClient (cache paylaşımı yok).
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

export function makeQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,            // test bekleme zamanını uzatmaz
        gcTime: 0,
        staleTime: 0,
      },
      mutations: {
        retry: false,
      },
    },
  });
}

export function withQueryClient(client?: QueryClient) {
  const qc = client ?? makeQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}
