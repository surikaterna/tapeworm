import { expect, test } from "vitest";
import { BSON, ObjectId } from "mongodb";
import { MongoQuarantineSourceReader, MongoQuarantineStore } from "../../index";
import { sourceReference } from "../../src/quarantine/validation";
import { record } from "../../src/validation";
import { commit } from "../support/fixtures";
import { quarantineFixture } from "./support/integration-fixture";
import { request } from "./support/fixtures";
import { eventually, mongo } from "../support/services";

test("unique identity capture is concurrent-idempotent, bounded, reference-only and never resets published history", async () => {
  const f = await quarantineFixture();
  try {
    const results = await Promise.all(Array.from({ length: 12 }, () => f.store.capture(f.reference, "message-too-large")));
    expect(new Set(results.map((item) => item.id)).size).toBe(1);
    expect(await f.mongodb.db.collection("quarantine").countDocuments()).toBe(1);
    const claimed = await f.store.claim(f.captured.id, request);
    if (claimed.kind !== "claimed") throw new Error("Claim missing");
    expect(await f.store.finish(f.captured.id, claimed.token, "published")).toBe(true);
    const replay = await f.store.capture(f.reference, "message-too-large");
    expect(replay.status).toBe("published"); expect(replay.attempts).toHaveLength(1);
    expect(replay.observations).toBe(14);
    await expect(f.store.capture({ ...f.reference, fingerprint: "a".repeat(64) }, "message-too-large")).rejects.toThrow("fingerprint");
    const doc: unknown = await f.mongodb.db.collection("quarantine").findOne({});
    expect(BSON.calculateObjectSize(record(doc))).toBeLessThan(8192);
    expect(record(doc)).not.toHaveProperty("events");
    await f.mongodb.db.collection("quarantine").updateOne({}, { $set: { observations: 2147483647 } });
    expect((await f.store.capture(f.reference, "message-too-large")).observations).toBe(2147483647);
  } finally { await f.mongodb.close(); }
});
test("scoped keyset list uses bounded indexed plans and rejects cross-feed cursors", async () => {
  const f = await quarantineFixture();
  try {
    for (let n = 2; n <= 5; n++) await f.store.capture(sourceReference(commit(n), f.scope), "message-too-large");
    const page = await f.store.list({ limit: 2, status: "quarantined" });
    expect(page.records).toHaveLength(2); expect(page.after).toBeDefined();
    const next = await f.store.list({ limit: 2, status: "quarantined", after: page.after });
    expect(next.records[0]?.id).not.toBe(page.records[0]?.id);
    const other = new MongoQuarantineStore(f.mongodb.db, "quarantine", { ...f.scope, feed: "other-oplog-feed" });
    await other.initialize(); expect((await other.list()).records).toEqual([]);
    await expect(other.list({ after: page.after, status: "quarantined" })).rejects.toThrow("cursor");
    await expect(f.store.list({ limit: 101 })).rejects.toThrow("limit");
    for (const status of [undefined, "quarantined"]) {
      const plan: unknown = await f.mongodb.db.collection("quarantine").find({ ...f.scope, ...(status ? { status } : {}) })
        .hint(status ? "quarantine_status_id" : "quarantine_scope_id").sort({ _id: 1 }).limit(2).explain("executionStats");
      expect(JSON.stringify(plan)).toContain("IXSCAN"); expect(JSON.stringify(plan)).not.toContain("COLLSCAN");
      expect(record(record(plan).executionStats).totalDocsExamined).toBe(2);
    }
  } finally { await f.mongodb.close(); }
});
test("source lookup requires preexisting unique id index and rejects missing, changed or tampered identity", async () => {
  const bare = await mongo();
  try { await expect(new MongoQuarantineSourceReader(bare.db, "commits", "feed").initialize()).rejects.toThrow("unique simple id"); }
  finally { await bare.close(); }
  const f = await quarantineFixture();
  try {
    expect((await f.source.read(f.reference)).id).toBe(f.value.id);
    const plan: unknown = await f.mongodb.db.collection("commits").find({ id: f.value.id }).hint("id_1").explain("executionStats");
    expect(JSON.stringify(plan)).toContain("IXSCAN"); expect(record(record(plan).executionStats).totalDocsExamined).toBe(1);
    await expect(f.source.read({ ...f.reference, streamId: "tampered" })).rejects.toThrow("fingerprint");
    await f.mongodb.db.collection("commits").updateOne({ id: f.value.id }, { $set: { domain: "mutated" } });
    await expect(f.source.read(f.reference)).rejects.toThrow("fingerprint");
    await f.mongodb.db.collection("commits").deleteOne({ id: f.value.id });
    await expect(f.source.read(f.reference)).rejects.toThrow();
  } finally { await f.mongodb.close(); }
});
test("server-time claims exclude competitors, expiry atomically retains unknown audit and fences stale completion", async () => {
  const f = await quarantineFixture(150);
  try {
    const claims = await Promise.all([f.store.claim(f.captured.id, request), f.store.claim(f.captured.id, request)]);
    const first = claims.find((claim) => claim.kind === "claimed");
    if (!first) throw new Error("Claim missing");
    expect(claims.map((claim) => claim.kind).sort()).toEqual(["busy", "claimed"]);
    const audit = first.record.attempts[0];
    expect(audit?.expiresAt.getTime()).toBe((audit?.startedAt.getTime() ?? 0) + 150);
    await eventually(async () => Boolean(await f.mongodb.db.collection("quarantine").findOne({
      _id: new ObjectId(f.captured.id), $expr: { $lte: ["$claimExpiresAt", "$$NOW"] } })));
    const second = await f.store.claim(f.captured.id, { actor: "$operator", reason: "$repair" });
    if (second.kind !== "claimed") throw new Error("Takeover missing");
    expect(second.record.attempts[0]).toMatchObject({ result: "outcome-unknown", diagnostic: "lease-expired" });
    expect(second.record.attempts[1]).toMatchObject({ actor: "$operator", reason: "$repair" });
    expect(await f.store.finish(f.captured.id, first.token, "published")).toBe(false);
    expect(await f.store.finish(f.captured.id, second.token, "rejected", "source-invalid")).toBe(true);
  } finally { await f.mongodb.close(); }
});
test("all 100 manual attempts remain audited, further claims refused, metadata stays well below BSON limit", async () => {
  const f = await quarantineFixture();
  try {
    for (let n = 0; n < 100; n++) {
      const claim = await f.store.claim(f.captured.id, { actor: "a".repeat(128), reason: "r".repeat(1024) });
      if (claim.kind !== "claimed") throw new Error("Unexpected claim refusal");
      expect(await f.store.finish(f.captured.id, claim.token, "rejected", "message-too-large")).toBe(true);
    }
    expect(await f.store.claim(f.captured.id, request)).toEqual({ kind: "attempt-limit" });
    const doc: unknown = await f.mongodb.db.collection("quarantine").findOne({});
    expect((await f.store.find(f.reference))?.attempts).toHaveLength(100);
    expect(BSON.calculateObjectSize(record(doc))).toBeLessThan(200000);
  } finally { await f.mongodb.close(); }
});
