import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          SECRETS_ENCRYPTION_KEY: "test-only-encryption-key".padEnd(48, "e"),
        },
      },
    }),
  ],
  test: {
    include: ["test/**/*.spec.ts"],
    maxWorkers: 1,
  },
});
