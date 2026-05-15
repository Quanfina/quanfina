export interface Term {
  key: string;
  short_name: string;
  tooltip: string;
  definition: string;
  source_book: string | null;
  source_author: string | null;
  source_year: number | null;
  quanfina_context: string;
  category: "technical" | "fundamental" | "strategy" | "risk";
}
