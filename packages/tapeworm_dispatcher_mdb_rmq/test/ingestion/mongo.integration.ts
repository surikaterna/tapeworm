import { expect, test } from "vitest";
import { Timestamp, UUID } from "mongodb";
import { MongoHistory } from "../../src/ingestion/history";
import { ChangeStreamSource, OplogSource } from "../../src/ingestion/live-source";
import { MongoResumeTokenStore } from "../../src/checkpoints/mongodb-store";
import { commit, token } from "../support/fixtures";
import { mongo } from "../support/services";
import { record } from "../../src/validation";

test("Mongo driver server boundary on EMPTY collection includes subsequent live inserts", async () => {
  const env = await mongo();
  const history = new MongoHistory(env.config);
  const source = new ChangeStreamSource(env.config);
  try {
    const boundary = await history.boundary();
    expect(boundary).toBeInstanceOf(Timestamp);
    expect(await history.upper()).toBeUndefined();
    await env.db.collection("commits").insertOne(commit(11), { writeConcern: { w: "majority" } });
    const iterator = source.watch({ kind: "boundary", mode: "changeStream", ts: boundary })[Symbol.asyncIterator]();
    const next = await iterator.next();
    if (next.done) throw new Error("Missing insert");
    expect(next.value.commit.id).toBe("commit-11");
    await iterator.return?.();
  } finally { await source.close(); await env.close(); }
});

test("finite indexed scan, ties retry inclusively and lower UUID inserted during scan reaches live", async () => {
  const env = await mongo();
  const history = new MongoHistory(env.config);
  const source = new ChangeStreamSource(env.config);
  try {
    await env.db.collection("commits").insertMany([commit(20), commit(30)]);
    const boundary = await history.boundary();
    const upper = await history.upper();
    await env.db.collection("commits").insertMany([commit(5), commit(40), { ...commit(20), id: "tie" }]);
    const scan = { phase: "scan" as const, lower: token(10), upper, boundary, startedAt: new Date() };
    const ids: string[] = [];
    for await (const value of history.scan(scan)) ids.push(value.id);
    expect(ids.sort()).toEqual(["commit-20", "commit-30", "tie"]);
    const resumed: string[] = [];
    for await (const value of history.scan({ ...scan, cursor: token(20) })) resumed.push(value.id);
    expect(resumed.sort()).toEqual(ids);
    const iterator = source.watch({ kind: "boundary", mode: "changeStream", ts: boundary })[Symbol.asyncIterator]();
    const seen: string[] = [];
    while (!seen.includes("commit-5")) { const next = await iterator.next(); if (!next.done) seen.push(next.value.commit.id); }
    await iterator.return?.();
    const explain: unknown = await env.db.collection("commits").find({ token: {
      $gt: new UUID(token(10)), $lte: new UUID(token(30)),
    } }).hint({ token: 1 }).sort({ token: 1 }).explain("executionStats");
    expect(JSON.stringify(explain)).toContain("IXSCAN");
    expect(JSON.stringify(explain)).not.toContain("COLLSCAN");
    expect(record(record(explain).executionStats).totalDocsExamined).toBe(3);
  } finally { await source.close(); await history.close(); await env.close(); }
});

test("accepted residual: retained primary delivers late lower UUID, historical UUID fallback omits it", async () => {
  const env = await mongo();
  const history = new MongoHistory(env.config);
  const source = new ChangeStreamSource(env.config);
  try {
    const boundary = await history.boundary();
    await env.db.collection("commits").insertOne(commit(20));
    const iterator = source.watch({ kind: "boundary", mode: "changeStream", ts: boundary })[Symbol.asyncIterator]();
    const first = await iterator.next();
    if (first.done) throw new Error("Missing first insert");
    const acknowledged = first.value.position;
    await iterator.return?.();
    await env.db.collection("commits").insertOne(commit(10));
    const resumed = source.watch(acknowledged)[Symbol.asyncIterator]();
    const next = await resumed.next();
    if (next.done) throw new Error("Missing resumed insert");
    expect(next.value.commit.id).toBe("commit-10");
    await resumed.return?.();
    const recoveryBoundary = await history.boundary();
    const ids: string[] = [];
    for await (const value of history.scan({ phase: "scan", lower: token(20), upper: await history.upper(),
      boundary: recoveryBoundary, startedAt: new Date() })) ids.push(value.id);
    expect(ids).not.toContain("commit-10");
  } finally { await source.close(); await env.close(); }
});

test("missing token index fails explicitly, never builds one or full scans", async () => {
  const env = await mongo();
  try {
    await env.db.collection("commits").dropIndex("token_1");
    await expect(new MongoHistory(env.config).upper()).rejects.toThrow("existing complete");
  } finally { await env.close(); }
});

test("Mongo store roundtrips BSON recovery and scoped checkpoints; legacy adoption explicit", async () => {
  const env = await mongo();
  try {
    const a = new MongoResumeTokenStore(env.db, "state", { checkpointKey: "a", feedId: "a" });
    const b = new MongoResumeTokenStore(env.db, "state", { checkpointKey: "b", feedId: "b" });
    const boundary = await new MongoHistory(env.config).boundary();
    const state = { version: 1 as const, feed: "a", updatedAt: new Date(), lastCommitToken: token(10),
      primary: { kind: "boundary" as const, mode: "changeStream" as const, ts: boundary },
      recovery: { phase: "scan" as const, boundary, lower: token(10), upper: token(20), startedAt: new Date() } };
    await a.save(state);
    expect(await a.load()).toEqual(state);
    expect(await b.load()).toBeNull();
    await expect(b.save(state)).rejects.toThrow("feed mismatch");
    const legacy = new MongoResumeTokenStore(env.db, "legacy");
    await legacy.save({ updatedAt: new Date(), lastCommitToken: token(10) });
    await expect(new MongoResumeTokenStore(env.db, "legacy", { feedId: "a" }).load()).rejects.toThrow("migration");
    const adopted = new MongoResumeTokenStore(env.db, "legacy", { feedId: "a", adoptLegacyCheckpoint: true });
    const old = await adopted.load();
    if (!old) throw new Error("Missing legacy state");
    await adopted.save(old);
    expect((await adopted.load())?.feed).toBe("a");
  } finally { await env.close(); }
});

test("explicit oplog mode uses server Timestamp and resumes direct inserts", async () => {
  const env = await mongo();
  const source = new OplogSource(env.config);
  try {
    const boundary = await new MongoHistory(env.config).boundary();
    await env.db.collection("commits").insertOne(commit(11));
    const iterator = source.watch({ kind: "boundary", mode: "oplog", ts: boundary })[Symbol.asyncIterator]();
    const first = await iterator.next();
    if (first.done) throw new Error("Missing oplog insert");
    expect(first.value.commit.id).toBe("commit-11");
    await iterator.return?.();
    await env.db.collection("commits").insertOne(commit(12));
    const resumed = source.watch(first.value.position)[Symbol.asyncIterator]();
    const next = await resumed.next();
    if (next.done) throw new Error("Missing resumed insert");
    expect(next.value.commit.id).toBe("commit-12");
    await resumed.return?.();
  } finally { await source.close(); await env.close(); }
});
