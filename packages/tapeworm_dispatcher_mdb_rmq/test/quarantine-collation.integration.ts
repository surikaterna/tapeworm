import { MongoClient, ObjectId, type Db } from "mongodb";
import { expect, test } from "vitest";
import { MongoQuarantineSourceReader, MongoQuarantineStore } from "../index";
import type { QuarantineScope } from "../index";
import { sourceReference } from "../src/quarantine/validation";
import { record } from "../src/validation";
import { commit } from "./fixtures";
import { request } from "./quarantine-fixtures";
import { mongoUri, unique } from "./services";

async function fixture(strength = 1) {
  const client = await new MongoClient(mongoUri, { monitorCommands: true }).connect();
  const db = client.db(unique());
  await db.createCollection("quarantine", { collation: { locale: "en", strength } });
  const commands: Record<string, unknown>[] = [];
  client.on("commandStarted", (event) => { const command: unknown = event.command; commands.push(record(command)); });
  return { db, commands, close: async () => { await db.dropDatabase(); await client.close(); } };
}
const scopePairs = [
  [{ feed: "Feed-A", sourceCollection: "commits" }, { feed: "feed-a", sourceCollection: "commits" }],
  [{ feed: "Cafe", sourceCollection: "commits" }, { feed: "Café", sourceCollection: "commits" }],
  [{ feed: "feed", sourceCollection: "Commits" }, { feed: "feed", sourceCollection: "commits" }],
] satisfies [QuarantineScope, QuarantineScope][];

function assertBinaryCommands(commands: Record<string, unknown>[]): void {
  for (const command of commands) {
    if (command.find || command.findAndModify) expect(command.collation).toEqual({ locale: "simple" });
    if (typeof command.update === "string") {
      if (!Array.isArray(command.updates)) throw new Error("Missing update statements");
      const statements: unknown[] = command.updates;
      for (const update of statements.map(record)) expect(update.collation).toEqual({ locale: "simple" });
    }
  }
}

test.each(scopePairs)("binary quarantine scope isolates list, find, capture and pagination: %j vs %j", async (left, right) => {
  const f = await fixture(left.feed === "Feed-A" ? 2 : 1);
  const a = new MongoQuarantineStore(f.db, "quarantine", left);
  const b = new MongoQuarantineStore(f.db, "quarantine", right);
  try {
    await a.initialize(); await b.initialize();
    const ar = sourceReference(commit(1), left); const br = sourceReference(commit(1), right);
    const first = await a.capture(ar, "message-too-large");
    expect((await b.list()).records).toEqual([]); expect(await b.find(br)).toBeNull();
    const other = await b.capture(br, "message-too-large"); expect(other.id).not.toBe(first.id);
    await a.capture(sourceReference(commit(2), left), "message-too-large");
    for (const status of [undefined, "quarantined"] as const) {
      const page = await a.list({ limit: 1, status });
      expect(page.records.map((value) => value.id)).toEqual([first.id]); expect(page.after).toBeDefined();
      const next = await a.list({ limit: 1, after: page.after, status });
      expect(next.records.map((value) => value.reference.commitId)).toEqual(["commit-2"]);
      expect((await b.list({ status })).records.map((value) => value.id)).toEqual([other.id]);
    }
    expect((await a.capture(ar, "message-too-large")).observations).toBe(2);
    expect((await b.find(br))?.observations).toBe(1);
    assertBinaryCommands(f.commands);
    const indexes: unknown[] = await f.db.collection("quarantine").listIndexes().toArray();
    for (const index of indexes.map(record).filter((value) => value.name !== "_id_")) expect(index.collation).toBeUndefined();
  } finally { await f.close(); }
});
test.each(scopePairs)("binary quarantine claims/completions cannot cross equivalent scopes: %j vs %j", async (left, right) => {
  const f = await fixture(left.feed === "Feed-A" ? 2 : 1);
  const a = new MongoQuarantineStore(f.db, "quarantine", left);
  const b = new MongoQuarantineStore(f.db, "quarantine", right);
  try {
    await a.initialize(); await b.initialize();
    const first = await a.capture(sourceReference(commit(1), left), "message-too-large");
    expect(await b.claim(first.id, request)).toEqual({ kind: "missing" });
    const claimed = await a.claim(first.id, request);
    if (claimed.kind !== "claimed") throw new Error("Missing owned claim");
    expect(await b.claim(first.id, request)).toEqual({ kind: "missing" });
    expect(await b.finish(first.id, claimed.token, "published")).toBe(false);
    const other = await b.capture(sourceReference(commit(1), right), "message-too-large");
    const own = await b.claim(other.id, request);
    if (own.kind !== "claimed") throw new Error("Missing distinct claim");
    expect(await b.finish(other.id, claimed.token, "published")).toBe(false);
    const wrongToken = claimed.token.toUpperCase();
    expect(await a.finish(first.id, wrongToken === claimed.token ? `${wrongToken}A` : wrongToken, "published")).toBe(false);
    expect(await a.finish(first.id, claimed.token, "published")).toBe(true);
    expect(await b.claim(first.id, request)).toEqual({ kind: "missing" });
    expect(await b.finish(other.id, own.token, "rejected", "message-too-large")).toBe(true);
    expect(await a.finish(first.id, claimed.token, "published")).toBe(false);
    assertBinaryCommands(f.commands);
  } finally { await f.close(); }
});
test("binary quarantine commit ids preserve case and accent distinctions within one scope", async () => {
  const f = await fixture(); const scope = { feed: "feed", sourceCollection: "commits" };
  const store = new MongoQuarantineStore(f.db, "quarantine", scope);
  try {
    await store.initialize();
    const ids = ["Commit-A", "commit-a", "Cafe", "Café"];
    for (const [n, id] of ids.entries()) {
      const reference = sourceReference({ ...commit(n), id }, scope);
      expect(await store.find(reference)).toBeNull();
      const captured = await store.capture(reference, "message-too-large");
      expect((await store.find(reference))?.id).toBe(captured.id);
    }
    expect((await store.list()).records.map((value) => value.reference.commitId)).toEqual(ids);
  } finally { await f.close(); }
});

test.each(["quarantine_identity", "legacy_unique", "quarantine_scope_id", "legacy_beside_binary"])("reject incompatible index %s before data access or index changes", async (name) => {
  const f = await fixture(); const scope = { feed: "feed", sourceCollection: "commits" };
  const collection = f.db.collection("quarantine");
  try {
    if (name === "legacy_beside_binary") await new MongoQuarantineStore(f.db, "quarantine", scope).initialize();
    await collection.createIndex({ feed: 1, sourceCollection: 1, commitId: 1 }, { name, unique: name !== "quarantine_scope_id" });
    const before: unknown[] = await collection.listIndexes().toArray();
    const store = new MongoQuarantineStore(f.db, "quarantine", scope);
    const reference = sourceReference(commit(1), scope); const id = new ObjectId().toHexString();
    const calls = [() => store.initialize(), () => store.find(reference), () => store.list(),
      () => store.capture(reference, "message-too-large"), () => store.claim(id, request), () => store.finish(id, "token", "published")];
    f.commands.length = 0;
    for (const call of calls) await expect(call()).rejects.toThrow("operator index migration");
    expect(f.commands.every((command) => command.listIndexes === "quarantine")).toBe(true);
    expect(await collection.listIndexes().toArray()).toEqual(before);
    expect(await collection.countDocuments()).toBe(0);
  } finally { await f.close(); }
});
test("unrelated linguistic nonunique indexes do not block binary quarantine initialization", async () => {
  const f = await fixture(); const scope = { feed: "feed", sourceCollection: "commits" };
  try {
    const collection = f.db.collection("quarantine");
    await collection.createIndex({ code: 1 }, { name: "operator_diagnostics" });
    const store = new MongoQuarantineStore(f.db, "quarantine", scope);
    const reference = sourceReference(commit(1), scope);
    const captured = await store.capture(reference, "message-too-large");
    expect((await store.find(reference))?.id).toBe(captured.id);
    const indexes: unknown[] = await collection.listIndexes().toArray();
    expect(indexes.map(record).find((index) => index.name === "operator_diagnostics")?.collation).toBeDefined();
  } finally { await f.close(); }
});

function indexBounds(value: unknown): unknown {
  const node = record(value);
  if (node.indexBounds) return node.indexBounds;
  if (node.inputStage) return indexBounds(node.inputStage);
  if (node.queryPlan) return indexBounds(node.queryPlan);
  throw new Error("Missing indexed point bounds");
}
async function explainRead(db: Db, command: Record<string, unknown>, id: string, exists: boolean): Promise<void> {
  expect(command.collation).toEqual({ locale: "simple" });
  expect(command.hint).toBe("id_1"); expect(command.filter).toEqual({ id });
  const result: unknown = await db.command({ explain: { find: "sources", filter: command.filter,
    hint: command.hint, collation: command.collation, limit: 1, singleBatch: true }, verbosity: "executionStats" });
  const plan = record(result); const stats = record(plan.executionStats);
  expect(stats.totalDocsExamined).toBe(exists ? 1 : 0);
  expect(stats.totalKeysExamined).toBe(exists ? 1 : 0);
  expect(record(indexBounds(record(plan.queryPlanner).winningPlan)).id).toEqual([`[${JSON.stringify(id)}, ${JSON.stringify(id)}]`]);
}
test.each([1, 2])("source binary lookup resolves exact case/accent ids with point bounds at strength %i", async (strength) => {
  const f = await fixture(); const scope = { feed: "feed", sourceCollection: "sources" };
  try {
    const sources = await f.db.createCollection("sources", { collation: { locale: "en", strength } });
    await sources.createIndex({ id: 1 }, { unique: true, collation: { locale: "simple" } });
    const ids = ["Commit-A", "commit-a", "Cafe", "Café"];
    const values = ids.map((id, n) => ({ ...commit(n), id }));
    await sources.insertMany(values);
    f.commands.length = 0;
    const reader = new MongoQuarantineSourceReader(f.db, "sources", scope.feed); await reader.initialize();
    expect(f.commands.map((command) => command.listIndexes)).toEqual(["sources"]);
    for (const value of values) {
      const reference = sourceReference(value, scope); f.commands.length = 0;
      expect(sourceReference(await reader.read(reference), scope)).toEqual(reference);
      const query = f.commands.find((command) => command.find === "sources");
      if (!query) throw new Error("Missing production source query");
      await explainRead(f.db, query, value.id, true);
    }
    const absent = sourceReference({ ...commit(5), id: "COMMIT-A" }, scope); f.commands.length = 0;
    await expect(reader.read(absent)).rejects.toThrow();
    const query = f.commands.find((command) => command.find === "sources");
    if (!query) throw new Error("Missing absent-source query");
    await explainRead(f.db, query, absent.commitId, false);
  } finally { await f.close(); }
});
