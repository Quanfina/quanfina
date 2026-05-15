"use client";

import { useQuery } from "@tanstack/react-query";
import type { Term } from "@/types/term";

async function fetchTerms(): Promise<Term[]> {
  const res = await fetch("/api/terms");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useTerms() {
  return useQuery({
    queryKey: ["terms"],
    queryFn: fetchTerms,
    staleTime: 5 * 60_000,
    retry: 1,
  });
}
