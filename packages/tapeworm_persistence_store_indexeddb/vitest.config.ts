import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  test: {
    globals: true,
    include: ["test/**/*.ts"],
    exclude: ["test/util.ts"],
  },
  resolve: {
    alias: {
      tapeworm: path.resolve(__dirname, "../tapeworm/index.ts"),
    },
  },
});
