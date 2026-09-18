import { deepStrictEqual } from "node:assert";
import { readFileSync } from "node:fs";
import { BSON, MongoClient, ObjectId, type CommandStartedEvent } from "mongodb";
import { expect, test } from "vitest";
import type { QuarantineDocument } from "../../src/quarantine/document";
import { MongoQuarantineClaims } from "../../src/quarantine/mongodb-claims";
import type { AttemptResult, DiagnosticCode } from "../../src/quarantine/types";
import { sourceReference } from "../../src/quarantine/validation";
import { record } from "../../src/validation";
import { commit, token } from "../support/fixtures";
import { mongoUri, unique } from "../support/services";

const scope = { feed: "claim-query-feed", sourceCollection: "commits" };
const id = new ObjectId("000000000000000000000001");
const request = { actor: "$actor", reason: "$reason" };
const past = new Date("2000-01-01T00:00:00.000Z");
const future = new Date("2100-01-01T00:00:00.000Z");
// Captured from real driver commands at 1ec5870 before extracting the claim helpers (redemeine-nqxf).
const golden: unknown = BSON.EJSON.parse(readFileSync(new URL("./quarantine-claim-query.golden.json", import.meta.url), "utf8"), { relaxed: false });

function document(): QuarantineDocument {
  return { _id: id, ...scope, version: 1, reference: sourceReference(commit(1), scope), commitId: "commit-1",
    code: "message-too-large", status: "quarantined", observations: 1, createdAt: past, attempts: [], attemptCount: 0 };
}

function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("Expected command array");
  return value;
}

function normalize(event: CommandStartedEvent): Record<string, unknown> {
  const value: unknown = event.command;
  // Clone BSON so monitoring cannot mutate the command sent to MongoDB.
  const cloned: unknown = BSON.deserialize(BSON.serialize(record(value)));
  const command = record(cloned);
  // Only session/transport metadata varies between otherwise identical real driver commands.
  delete command.lsid;
  delete command.$clusterTime;
  delete command.$db;
  delete command.txnNumber;
  if (command.findAndModify === "quarantine") {
    const stages = array(command.update);
    const start = record(record(stages[1]).$set);
    const generated: unknown = start.claimToken;
    expect(generated).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
    const audit = record(array(array(record(start.attempts).$concatArrays)[1])[0]);
    expect(audit.token).toBe(generated);
    start.claimToken = token(1);
    stages[1] = { ...record(stages[1]), $set: start };
    array(array(record(start.attempts).$concatArrays)[1])[0] = { ...audit, token: token(1) };
  }
  return command;
}

async function fixture() {
  const client = await new MongoClient(mongoUri, { monitorCommands: true }).connect();
  const db = client.db(unique());
  const collection = db.collection<QuarantineDocument>("quarantine");
  const commands: Record<string, unknown>[] = [];
  client.on("commandStarted", (event: CommandStartedEvent) => { commands.push(normalize(event)); });
  return { collection, commands, claims: new MongoQuarantineClaims(collection, scope, 60000),
    close: async () => { await db.dropDatabase(); await client.close(); } };
}

function equivalent(names: string[], commands: Record<string, unknown>[]): void {
  const expected = names.map((name) => BSON.serialize(record(record(golden)[name])));
  deepStrictEqual(commands.map((command) => BSON.serialize(command)), expected);
}

test.each(["quarantined", "expired", "busy", "missing", "published", "attempt-limit"] as const)(
  "claim commands match pre-refactor baseline: %s", async (state) => {
    const f = await fixture();
    try {
      const doc = document();
      if (state === "published") { doc.status = "published"; doc.attemptCount = 100; }
      if (state === "attempt-limit") doc.attemptCount = 100;
      if (state === "expired" || state === "busy") {
        doc.status = "claimed"; doc.claimToken = token(2); doc.claimExpiresAt = state === "expired" ? past : future;
        doc.attemptCount = 1;
        doc.attempts = [{ token: token(2), ...request, startedAt: past, expiresAt: doc.claimExpiresAt }];
      }
      if (state !== "missing") await f.collection.insertOne(doc);
      f.commands.length = 0;
      const result = await f.claims.claim(id.toHexString(), request);
      const success = state === "quarantined" || state === "expired";
      expect(result.kind).toBe(success ? "claimed" : state === "published" ? "already-published" : state);
      expect(f.commands).toHaveLength(success ? 1 : 2);
      equivalent(success ? ["claim"] : ["claim", "fallback"], f.commands);
      if (result.kind === "claimed") {
        expect(result.record.attempts.at(-1)).toMatchObject(request);
        if (state === "expired") expect(result.record.attempts[0]).toMatchObject({ result: "outcome-unknown", diagnostic: "lease-expired" });
      }
    } finally { await f.close(); }
  },
);

const completions: [AttemptResult, DiagnosticCode | undefined][] = [
  ["published", undefined], ["rejected", undefined], ["outcome-unknown", undefined],
  ["published", "publication-failed"], ["rejected", "message-too-large"], ["outcome-unknown", "publication-failed"],
];
test.each(completions)("finish commands match pre-refactor baseline: %s / %s", async (result, diagnostic) => {
  const f = await fixture();
  try {
    const doc = document();
    doc.status = "claimed"; doc.claimToken = token(1); doc.claimExpiresAt = future; doc.attemptCount = 1;
    doc.attempts = [{ token: token(1), ...request, startedAt: past, expiresAt: future }];
    await f.collection.insertOne(doc);
    f.commands.length = 0;
    expect(await f.claims.finish(id.toHexString(), token(1), result, diagnostic)).toBe(true);
    expect(f.commands).toHaveLength(1);
    equivalent([`${result}/${diagnostic ?? "none"}`], f.commands);
    const finished = await f.collection.findOne({ _id: id });
    expect(finished?.status).toBe(result === "published" ? "published" : "quarantined");
    expect(finished).not.toHaveProperty("claimToken");
    expect(finished).not.toHaveProperty("claimExpiresAt");
    expect(finished?.attempts[0]).toMatchObject({ result, ...(diagnostic ? { diagnostic } : {}) });
    expect(finished?.attempts[0]?.finishedAt).toBeInstanceOf(Date);
  } finally { await f.close(); }
});

test.each(["stale", "expired"])("refused %s finish retains the same atomic command and document", async (state) => {
  const f = await fixture();
  try {
    const doc = document();
    doc.status = "claimed"; doc.claimToken = state === "stale" ? token(2) : token(1);
    doc.claimExpiresAt = state === "expired" ? past : future; doc.attemptCount = 1;
    doc.attempts = [{ token: doc.claimToken, ...request, startedAt: past, expiresAt: doc.claimExpiresAt }];
    await f.collection.insertOne(doc);
    f.commands.length = 0;
    expect(await f.claims.finish(id.toHexString(), token(1), "published")).toBe(false);
    equivalent(["published/none"], f.commands);
    deepStrictEqual(await f.collection.findOne({ _id: id }), doc);
  } finally { await f.close(); }
});
