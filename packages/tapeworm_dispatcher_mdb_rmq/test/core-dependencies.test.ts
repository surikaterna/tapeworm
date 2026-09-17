import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve, relative, sep } from "node:path";
import ts from "typescript";
import { expect, test } from "vitest";

function specifier(node: ts.Node): string | undefined {
  if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier
    && ts.isStringLiteral(node.moduleSpecifier)) return node.moduleSpecifier.text;
  if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument)
    && ts.isStringLiteral(node.argument.literal)) return node.argument.literal.text;
  if (ts.isExternalModuleReference(node) && ts.isStringLiteral(node.expression)) return node.expression.text;
  if (!ts.isCallExpression(node)) return undefined;
  const call = node.expression;
  if (call.kind !== ts.SyntaxKind.ImportKeyword && !(ts.isIdentifier(call) && call.text === "require")) return undefined;
  const arg = node.arguments[0];
  return arg && (ts.isStringLiteral(arg) || ts.isNoSubstitutionTemplateLiteral(arg)) ? arg.text : undefined;
}

function imports(source: string): string[] {
  const file = ts.createSourceFile("module.ts", source, ts.ScriptTarget.Latest, true);
  const result: string[] = [];
  function visit(node: ts.Node): void {
    const name = specifier(node);
    if (name) result.push(name);
    ts.forEachChild(node, visit);
  }
  visit(file); return result;
}

function localPath(from: string, name: string): string | undefined {
  if (!name.startsWith(".")) return undefined;
  const base = resolve(dirname(from), name).replace(/\.js$/, "");
  const path = [base, `${base}.ts`, resolve(base, "index.ts")].find((candidate) => candidate.endsWith(".ts") && existsSync(candidate));
  if (!path) throw new Error(`Unresolved local dependency ${name} from ${from}`);
  return path;
}

test("AST dependency reader covers runtime/type imports, reexports, import queries and literal loaders", () => {
  expect(imports(`import { a } from './a'; import type { B } from './b'; export * from './c';
    export type { D } from './d'; type E = import('./e').E; import f = require('./f');
    const g = require('./g'); const h = import('./h.js'); const ignored = "./not-an-import";`))
    .toEqual(["./a", "./b", "./c", "./d", "./e", "./f", "./g", "./h.js"]);
  expect(localPath(resolve("src/dispatcher.ts"), "./delivery.js")).toBe(resolve("src/delivery.ts"));
});

test("core transitive source graph cannot reach optional implementation or root barrel, including type edges", () => {
  const root = resolve("src"); const barrel = resolve("index.ts");
  const pending = ["dispatcher", "recovery-watcher", "delivery", "types", "dispatcher-config", "delivery-failure", "delivery-halted"]
    .map((name) => resolve(root, `${name}.ts`));
  const visited = new Set<string>();
  while (pending.length) {
    const path = pending.pop(); if (!path || visited.has(path)) continue;
    expect(path, "core must not bridge through the root feature barrel").not.toBe(barrel);
    expect(relative(root, path).split(sep), "optional feature reached from core").not.toContain("quarantine");
    expect(path.startsWith(`${root}${sep}`)).toBe(true);
    visited.add(path);
    for (const name of imports(readFileSync(path, "utf8"))) {
      expect(name).not.toBe("tapeworm_dispatcher_mdb_rmq");
      expect(name.startsWith("tapeworm_dispatcher_mdb_rmq/")).toBe(false);
      const dependency = localPath(path, name);
      if (dependency) pending.push(dependency);
    }
  }
  for (const required of ["publisher", "publication-policy", "validation", "history", "live-source", "feed", "resume/types"]) {
    expect(visited.has(resolve(root, `${required}.ts`))).toBe(true);
  }
  expect(visited.size).toBeGreaterThan(15);
  console.info(`Core AST graph: ${visited.size} local modules; optional-feature/root-barrel edges: 0`);
});
