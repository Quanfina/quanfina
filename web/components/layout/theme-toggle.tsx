"use client";

import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";

const THEMES = ["light", "dark", "system"] as const;
type Theme = (typeof THEMES)[number];

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <div className="flex gap-1">
      {THEMES.map((t: Theme) => (
        <Button
          key={t}
          variant={theme === t ? "default" : "outline"}
          size="sm"
          onClick={() => setTheme(t)}
          className="capitalize"
        >
          {t}
        </Button>
      ))}
    </div>
  );
}
