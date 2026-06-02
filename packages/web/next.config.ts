import type { NextConfig } from "next";

function parseAllowedDevOrigins(): string[] {
  const configured = process.env.SPREADS_WEB_ALLOWED_DEV_ORIGINS ?? "";
  const values = ["localhost", "127.0.0.1", ...configured.split(",")]
    .map((value) => value.trim())
    .filter(Boolean);
  return Array.from(new Set(values));
}

const nextConfig: NextConfig = {
  output: "standalone",
  allowedDevOrigins: parseAllowedDevOrigins(),
};

export default nextConfig;
