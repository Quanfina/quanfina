"use client";

import { MoreVertical } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { Trade } from "@/types/trade";

type Props = {
  data?: Trade;
  onEdit: (trade: Trade) => void;
  onClose: (trade: Trade) => void;
  onDelete: (trade: Trade) => void;
  onEditStop: (trade: Trade) => void;
};

export function TradeRowActions({ data, onEdit, onClose, onDelete, onEditStop }: Props) {
  if (!data) return null;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        className="p-1 rounded hover:bg-accent transition-colors outline-none"
        aria-label="İşlemler"
      >
        <MoreVertical size={14} />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-40">
        {data.status === "open" && (
          <>
            <DropdownMenuItem onClick={() => onEditStop(data)}>
              Stop / Hedef Düzenle
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => onClose(data)}>
              Kapat
            </DropdownMenuItem>
          </>
        )}
        <DropdownMenuItem onClick={() => onEdit(data)}>
          Düzenle
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={() => onDelete(data)}
          className="text-destructive focus:text-destructive"
        >
          Sil
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
