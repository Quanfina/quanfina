"use client";

import { HelpCircle } from "lucide-react";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";

/**
 * P425 (31 May 2026): Soru işareti (?) tooltip — gösterge açıklamaları için DRY.
 * Sn. Ferit talimat: "yaptığın göstergeleri tooltip şeklinde yani soru
 * işaretleri ile açıklamalarını ekle". base-ui Tooltip render={} pattern
 * (TermTooltip ile aynı — React 19 uyumlu).
 */
interface HelpTipProps {
  text: string;
  size?: number;
  label?: string;
}

export function HelpTip({ text, size = 13, label = "Açıklama" }: HelpTipProps) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            className="inline-flex items-center align-middle text-muted-foreground hover:text-foreground transition-colors focus:outline-none cursor-help"
            aria-label={label}
            data-testid="help-tip"
          />
        }
      >
        <HelpCircle size={size} strokeWidth={1.75} />
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs leading-relaxed">
        {text}
      </TooltipContent>
    </Tooltip>
  );
}
