import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "node:path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "."),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    include: ["__tests__/**/*.test.{ts,tsx}", "hooks/**/*.test.{ts,tsx}", "components/**/*.test.{ts,tsx}", "lib/**/*.test.{ts,tsx}"],
    // P389: jsdom environment setup ~28s, bazı async-heavy testler (özellikle
    // AddTradeDialog 14 hook mock + child stub) varsayılan 5s timeout'a takiliyor.
    // P388 sirasinda yakalandi (pre-existing flakiness). Global 15s ile flaky'lik
    // kapatildi — hizli testler degisiklik gormez, yavaslar nefes alir.
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
