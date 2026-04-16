import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  test: {
    include: ["**/*.{test,spec}.?[jt]s", "test/**/*.[jt]s"],
  },
  resolve: {
    alias: {
      tapeworm: path.resolve(__dirname, "index.ts"),
    },
  },
});
