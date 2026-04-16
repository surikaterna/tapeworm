// Append CJS backward-compatibility shim to compiled ESM output.
//
// Without this, `require('tapeworm_persistence_store_indexeddb')` returns
// `{ default: IDBPersistence, ... }` instead of the constructor directly.
// The shim restores the legacy behavior:
//   `const IDBPersistence = require('tapeworm_persistence_store_indexeddb')`
//
// This runs as a postbuild step so the source stays pure ESM (no
// `module.exports` that would conflict with Vite/vitest).

const fs = require("fs");
const path = require("path");

const file = path.join(__dirname, "..", "dist", "index.js");
const src = fs.readFileSync(file, "utf8");

const shim = [
  "",
  "// CJS backward compat — added by scripts/postbuild.js",
  "module.exports = exports.default;",
  "Object.assign(module.exports, exports);",
  "module.exports.default = exports.default;",
  "",
].join("\n");

fs.writeFileSync(file, src + shim);
