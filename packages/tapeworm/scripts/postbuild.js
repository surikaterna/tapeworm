// Append CJS backward-compatibility shim to compiled ESM output.
//
// Without this, `require('tapeworm')` returns `{ default: EventStore, ... }`
// instead of the EventStore constructor directly. The shim restores the
// legacy behavior: `const EventStore = require('tapeworm')` works, and
// named exports like `require('tapeworm').Commit` remain accessible because
// EventStore already carries them as own properties (set in event_store.ts).
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
