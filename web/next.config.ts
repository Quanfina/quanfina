import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
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
