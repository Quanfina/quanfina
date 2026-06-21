import type { NextConfig } from "next";

// P572 (20 Haz 2026 deploy): prod'da /api/* proxy hedefi API_URL env'inden (Cloud Run
// api servisi); lokal'de localhost:8000 (geriye uyum). standalone output Docker imajı için.
const API_PROXY_TARGET = process.env.API_URL || "http://localhost:8000";

const nextConfig: NextConfig = {
  output: "standalone",
  // Next 16 `next build` build-time lint YAPMAZ (eslint config anahtari da kalkti) — lint
  // ayri CI/lokal'de. unrs-resolver build-script'i --ignore-scripts ile atlandi (sorun yok).
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${API_PROXY_TARGET}/api/:path*`,
      },
    ];
  },
  // Dev runs on webpack (see package.json "dev"). Force an in-memory webpack
  // cache: the on-disk PackFileCacheStrategy .pack.gz files race and vanish
  // under this machine's real-time AV scan, crashing dev with ENOENT.
  webpack: (config, { dev }) => {
    if (dev) {
      config.cache = { type: "memory" };
    }
    return config;
  },
};

export default nextConfig;
