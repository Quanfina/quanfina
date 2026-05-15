"use client";

import { MoreVertical } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { WatchlistRow } from "@/types/watchlist";
import { canPromote, canDemote } from "@/lib/watchlist-status";

type Props = {
  data?: WatchlistRow;
  onPromote: (row: WatchlistRow) => void;
  onDemote: (row: WatchlistRow) => void;
  onEditNote: (row: WatchlistRow) => void;
  onDelete: (row: WatchlistRow) => void;
};

export function RowActionsRenderer({
  data,
  onPromote,
  onDemote,
  onEditNote,
  onDelete,
}: Props) {
  if (!data) return null;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        className="p-1 rounded hover:bg-accent transition-colors outline-none"
        aria-label="İşlemler"
      >
        <MoreVertical size={14} />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-44">
        {canPromote(data.status) && (
          <DropdownMenuItem onClick={() => onPromote(data)}>
            Statüyü Yükselt
          </DropdownMenuItem>
        )}
        {canDemote(data.status) && (
          <DropdownMenuItem onClick={() => onDemote(data)}>
            Statüyü Düşür
          </DropdownMenuItem>
        )}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => onEditNote(data)}>
          Not Düzenle
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={() => onDelete(data)}
          className="text-destructive focus:text-destructive"
        >
          Listeden Çıkar
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
