"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import type { LucideIcon } from "lucide-react";
import {
  Menu, Home, BarChart2, TrendingUp, Activity, Globe,
  ListChecks, NotebookText, ScanLine, Layers, ChevronDown,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { ThemeToggle } from "@/components/layout/theme-toggle";
import { NavLink } from "@/components/layout/nav-link";

// KARAR #486 (20 May 2026): Sn. Ferit talimat - "minervini ve carr stratejiler basligi
// altinda acilir kapanir sekilde sol menude barindar". Strateji sayfalari grup altinda
// toplandi. Akilli default: aktif route grup icindeyse acik baslar; manuel toggle
// localStorage'da saklanir (kullanici tercihi korunur). KARAR #485 sira bozulmadi.
// KARAR #491 (20 May 2026 ~16:30): /api-test sidebar'dan kaldirildi - dev/debug
// sayfasi, trader is akisina yabanci. Sayfa SILINMEDI, URL ile (/api-test) erisilebilir.
// Kural #18 Pasif Oge Cikarma somut uygulama (sidebar nav alanindan cikar, dosya kalir).
// Tetik: Sn. Ferit "amaci ne" sorusu -> AI karar (Kural #23 otonom).
type NavLeaf = { kind: "leaf"; href: string; label: string; icon: LucideIcon };
type NavGroup = { kind: "group"; id: string; label: string; icon: LucideIcon; children: NavLeaf[] };
type NavItem = NavLeaf | NavGroup;

const NAV_ITEMS: NavItem[] = [
  { kind: "leaf", href: "/",               label: "Ana Sayfa",     icon: Home         },
  { kind: "leaf", href: "/piyasa-durumu",  label: "Piyasa Durumu", icon: Globe        },
  { kind: "leaf", href: "/signals",        label: "Sinyaller",     icon: Activity     },
  { kind: "leaf", href: "/watchlist",      label: "İzleme Listesi", icon: ListChecks  },
  { kind: "leaf", href: "/screens",        label: "Tarama",         icon: ScanLine    },
  { kind: "leaf", href: "/journal",        label: "İşlem Günlüğü", icon: NotebookText },
  {
    kind: "group",
    id: "strategies",
    label: "Stratejiler",
    icon: Layers,
    children: [
      { kind: "leaf", href: "/minervini", label: "Minervini", icon: BarChart2  },
      { kind: "leaf", href: "/carr",      label: "Carr",      icon: TrendingUp },
    ],
  },
];
// /api-test KARAR #491 ile sidebar'dan kaldirildi - URL ile erisilebilir.

const STORAGE_PREFIX = "sidebar-group-";

function SidebarContent() {
  const pathname = usePathname();
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({});

  // Akilli default + localStorage geri okuma (kullanici tercihi koruma).
  useEffect(() => {
    const initial: Record<string, boolean> = {};
    for (const item of NAV_ITEMS) {
      if (item.kind === "group") {
        const stored = typeof window !== "undefined"
          ? window.localStorage.getItem(STORAGE_PREFIX + item.id)
          : null;
        if (stored !== null) {
          initial[item.id] = stored === "1";
        } else {
          // Default: aktif route grup icindeyse acik
          initial[item.id] = item.children.some((c) => pathname === c.href);
        }
      }
    }
    setOpenGroups(initial);
  }, [pathname]);

  const toggle = (id: string) => {
    setOpenGroups((prev) => {
      const next = { ...prev, [id]: !prev[id] };
      if (typeof window !== "undefined") {
        window.localStorage.setItem(STORAGE_PREFIX + id, next[id] ? "1" : "0");
      }
      return next;
    });
  };

  return (
    <div className="flex flex-col h-full">
      <div className="px-5 py-5 border-b">
        <span className="text-xl font-bold tracking-tight">QUANFINA</span>
      </div>

      <nav className="flex-1 px-3 py-4 flex flex-col gap-0.5">
        {NAV_ITEMS.map((item) => {
          if (item.kind === "leaf") {
            const Icon = item.icon;
            return (
              <NavLink key={item.href} href={item.href}>
                <Icon size={15} strokeWidth={1.75} />
                {item.label}
              </NavLink>
            );
          }
          // Group: collapsible header + nested children
          const Icon = item.icon;
          const isOpen = openGroups[item.id] ?? false;
          return (
            <div key={item.id} className="flex flex-col">
              <button
                type="button"
                onClick={() => toggle(item.id)}
                aria-expanded={isOpen}
                className="flex items-center gap-2.5 rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
              >
                <Icon size={15} strokeWidth={1.75} />
                <span className="flex-1 text-left">{item.label}</span>
                <ChevronDown
                  size={14}
                  className={`transition-transform duration-200 ${isOpen ? "rotate-0" : "-rotate-90"}`}
                />
              </button>
              {isOpen && (
                <div className="ml-3 mt-0.5 flex flex-col gap-0.5 border-l border-border/60 pl-2">
                  {item.children.map((child) => {
                    const ChildIcon = child.icon;
                    return (
                      <NavLink key={child.href} href={child.href}>
                        <ChildIcon size={15} strokeWidth={1.75} />
                        {child.label}
                      </NavLink>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      <div className="px-4 py-4 border-t">
        <ThemeToggle />
      </div>
    </div>
  );
}

export function Sidebar() {
  const [open, setOpen] = useState(false);

  return (
    <>
      {/* Desktop sidebar */}
      <aside className="hidden md:flex flex-col w-64 shrink-0 border-r bg-sidebar sticky top-0 h-screen">
        <SidebarContent />
      </aside>

      {/* Mobile: header bar */}
      <div className="md:hidden fixed top-0 inset-x-0 z-40 h-14 border-b bg-background flex items-center px-4 gap-3">
        <Button variant="ghost" size="icon" onClick={() => setOpen(true)}>
          <Menu size={18} />
        </Button>
        <span className="font-bold tracking-tight">QUANFINA</span>
      </div>

      {/* Mobile: sheet */}
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="left" className="p-0 w-64">
          <SidebarContent />
        </SheetContent>
      </Sheet>
    </>
  );
}
