import { defineConfig } from "vitest/config";
export default defineConfig({ test: { include: ["test/**/*.integration.ts"],
  testTimeout: 30000, hookTimeout: 30000, fileParallelism: false } });
