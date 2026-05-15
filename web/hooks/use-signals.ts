import { useQuery } from "@tanstack/react-query";
import type { Signal } from "@/types/signal";

export function useSignals() {
  return useQuery<Signal[]>({
    queryKey: ["signals"],
    queryFn: async () => {
      const res = await fetch("/api/signals");
      if (!res.ok) throw new Error("Sinyal verisi alınamadı");
      return res.json();
    },
  });
}
