import * as React from "react"

import { cn } from "@/lib/utils"

/**
 * Sprint 4-bis sonrası — Skeleton loading placeholder.
 * Markets360 4. dalga (p-skeleton 16 kullanım) + shadcn/ui stili sentezi.
 * Kural #20 sonucu: AG Grid loadingOverlayComponent ile birlikte kullanılır.
 */
function Skeleton({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="skeleton"
      className={cn("bg-muted/60 animate-pulse rounded-md", className)}
      {...props}
    />
  )
}

export { Skeleton }
