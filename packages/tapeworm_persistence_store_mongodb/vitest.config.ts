import { defineConfig } from "vitest/config";
import path from "node:path";

const pkgRoot = path.resolve(__dirname);
const srcIndex = path.resolve(__dirname, "index.ts");

export default defineConfig({
  test: {
    globals: true,
    include: ["test/**/*.spec.ts", "test/**/*.test.ts"],
  },
  resolve: {
    alias: {
      tapeworm: path.resolve(__dirname, "../tapeworm/index.ts"),
    },
  },
  plugins: [
    {
      name: "resolve-self-package",
      enforce: "pre",
      resolveId(source, importer) {
        if (!importer) return null;
        if (source === ".." || source === "../..") {
          const resolved = path.resolve(path.dirname(importer), source);
          if (path.resolve(resolved) === pkgRoot) {
            return srcIndex;
          }
        }
        return null;
      },
    },
  ],
});
