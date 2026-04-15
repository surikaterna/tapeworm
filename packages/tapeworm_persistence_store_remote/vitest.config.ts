import { defineConfig } from "vitest/config";
import path from "path";

export default defineConfig({
  test: {
    globals: true,
    include: ["test/**/*.ts"],
    exclude: ["test/__mocks__/**"],
  },
  resolve: {
    alias: {
      "@surikat/job-queue": path.resolve(
        __dirname,
        "test/__mocks__/job-queue.ts",
      ),
    },
  },
});
