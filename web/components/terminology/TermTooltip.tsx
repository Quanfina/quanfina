"use client";

import { useState } from "react";
import { HelpCircle } from "lucide-react";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { useTerms } from "@/hooks/use-terms";

interface TermTooltipProps {
  termKey: string;
  children: React.ReactNode;
}

const CATEGORY_LABELS: Record<string, string> = {
  technical: "Teknik",
  fundamental: "Temel",
  strategy: "Strateji",
  risk: "Risk",
};

export function TermTooltip({ termKey, children }: TermTooltipProps) {
  const [open, setOpen] = useState(false);
  const { data: terms } = useTerms();
  const term = terms?.find((t) => t.key === termKey);

  return (
    <>
      <span className="inline-flex items-center gap-0.5">
        {children}
        <Tooltip>
          <TooltipTrigger
            render={
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setOpen(true);
                }}
                className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors focus:outline-none"
                aria-label={`${termKey} terimini açıkla`}
              />
            }
          >
            <HelpCircle size={11} strokeWidth={1.75} />
          </TooltipTrigger>
          <TooltipContent side="top">
            {term ? term.tooltip : "Yükleniyor..."}
          </TooltipContent>
        </Tooltip>
      </span>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <div className="flex items-center gap-2">
              <DialogTitle>{term?.short_name ?? termKey}</DialogTitle>
              {term?.category && (
                <Badge variant="outline" className="text-xs font-normal">
                  {CATEGORY_LABELS[term.category] ?? term.category}
                </Badge>
              )}
            </div>
          </DialogHeader>
          <div className="flex flex-col gap-3 text-sm">
            <p className="leading-relaxed text-foreground">
              {term?.definition ?? "Tanım yüklenemedi."}
            </p>
            {term?.quanfina_context && (
              <div className="rounded-md border border-border bg-muted/40 px-3 py-2">
                <p className="text-xs font-medium text-muted-foreground mb-0.5">
                  Quanfina&apos;da kullanımı
                </p>
                <p className="text-xs leading-relaxed text-foreground">
                  {term.quanfina_context}
                </p>
              </div>
            )}
            {term?.source_book && (
              <p className="text-xs text-muted-foreground">
                Kaynak: <span className="italic">{term.source_book}</span>
                {term.source_author && ` — ${term.source_author}`}
                {term.source_year && ` (${term.source_year})`}
              </p>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
