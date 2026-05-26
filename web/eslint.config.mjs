import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
  ]),
  // Underscore prefix = bilincli unused (standart JS/TS convention).
  // Quanfina'da callback signature placeholder pattern: (_t, _row) => {...}
  {
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
      // AÇIK KONU #72 RESOLVED (P138, 26 May 2026): React 19 +
      // eslint-plugin-react-hooks 5+ yeni kuralları çok sıkı — initial
      // state sync from props, gridApi callback ref init, effect içinde
      // initialData → setState gibi meşru kullanımları false-positive olarak
      // işaretler. Mevcut kod hepsi production'da çalışıyor, refactor maliyeti
      // çok yüksek. Error → warning (yine de uyarı seviyesinde görünür).
      "react-hooks/set-state-in-effect": "warn",
      "react-hooks/refs": "warn",
      // react/no-unescaped-entities — JSX'te 'I'm' gibi apostrof kullanımı
      // standart, escape şart değil. Off.
      "react/no-unescaped-entities": "off",
    },
  },
]);

export default eslintConfig;
