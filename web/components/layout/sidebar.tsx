"use client";

import { useState } from "react";
import { Menu, Home, BarChart2, TrendingUp, Activity, FlaskConical, Globe } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { ThemeToggle } from "@/components/layout/theme-toggle";
import { NavLink } from "@/components/layout/nav-link";

// KARAR #347: Sinyaller > Piyasa Durumu > diğerleri
const NAV_ITEMS = [
  { href: "/",               label: "Ana Sayfa",      icon: Home         },
  { href: "/signals",        label: "Sinyaller",      icon: Activity     },
  { href: "/piyasa-durumu",  label: "Piyasa Durumu",  icon: Globe        },
  { href: "/minervini",      label: "Minervini",      icon: BarChart2    },
  { href: "/carr",           label: "Carr",           icon: TrendingUp   },
  { href: "/api-test",       label: "API Test",       icon: FlaskConical },
];

function SidebarContent() {
  return (
    <div className="flex flex-col h-full">
      <div className="px-5 py-5 border-b">
        <span className="text-xl font-bold tracking-tight">QUANFINA</span>
      </div>

      <nav className="flex-1 px-3 py-4 flex flex-col gap-0.5">
        {NAV_ITEMS.map(({ href, label, icon: Icon }) => (
          <NavLink key={href} href={href}>
            <Icon size={15} strokeWidth={1.75} />
            {label}
          </NavLink>
        ))}
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
