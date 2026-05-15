"use client";

import type { IHeaderParams } from "ag-grid-community";
import { TermTooltip } from "@/components/terminology/TermTooltip";

interface TermHeaderParams extends IHeaderParams {
  termKey: string;
}

export function TermHeaderComponent({ displayName, termKey }: TermHeaderParams) {
  return (
    <div className="flex items-center gap-0.5 h-full">
      <TermTooltip termKey={termKey}>{displayName}</TermTooltip>
    </div>
  );
}
