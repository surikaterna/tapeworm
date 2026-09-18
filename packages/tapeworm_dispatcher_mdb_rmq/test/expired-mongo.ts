import { randomBytes } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { Db, Timestamp } from "mongodb";
import { mongo } from "./services";
import { MongoHistory } from "../src/history";
import { ChangeStreamSource } from "../src/live-source";
import { commit } from "./fixtures";
import { record, timestamp } from "../src/validation";

const payloadBytes = 512 * 1024;
const initialWrites = 80;
const extraWriteLimit = 48;
const paceMs = 250;
const rolloverMs = 20000;
const diagnosticMs = 500;

function remaining(deadline: number): number {
  const ms = Math.floor(deadline - performance.now());
  if (ms <= 0) throw new Error("Expiry fixture rollover budget exhausted");
  return ms;
}

async function oplogEdge(local: Db, direction: 1 | -1, deadline: number) {
  const entry = await local.collection<Record<string, unknown>>("oplog.rs")
    .findOne({}, { sort: { $natural: direction }, timeoutMS: remaining(deadline) });
  return entry ? timestamp(entry.ts) : undefined;
}

async function collectionStats(db: Db, collection: string, deadline: number) {
  const stats: unknown = await db.command({ collStats: collection }, { timeoutMS: remaining(deadline) });
  const { size, count, maxSize } = record(stats);
  return { size, count, maxSize };
}

async function diagnostics(db: Db, end: number) {
  const deadline = Math.min(end, performance.now() + diagnosticMs);
  const local = db.client.db("local");
  const collect = async (operation: () => Promise<unknown>) => {
    try { return await operation(); }
    catch (error: unknown) { return { unavailable: error instanceof Error ? error.name : "UnknownError" }; }
  };
  const [latest, oplog, noise, recovery] = await Promise.all([
    collect(() => oplogEdge(local, -1, deadline)),
    collect(() => collectionStats(local, "oplog.rs", deadline)),
    collect(() => collectionStats(db, "noise", deadline)),
    collect(async () => {
      const status: unknown = await db.admin().command({ serverStatus: 1 }, { timeoutMS: remaining(deadline) });
      const { oplogTruncation, storageEngine } = record(status);
      return { oplogTruncation, storageEngine };
    }),
  ]);
  return { latest, oplog, noise, recovery };
}

async function rollOplog(db: Db, boundary: Timestamp, initialElapsedMs: number) {
  const started = performance.now();
  const deadline = started + rolloverMs;
  const workDeadline = deadline - diagnosticMs;
  const local = db.client.db("local");
  let extraWrites = 0;
  let oldest: Timestamp | undefined;
  const evidence = () => ({ boundary, oldest, initialElapsedMs, elapsedMs: performance.now() - started,
    extraWrites, generatedPayloadBytes: (initialWrites + extraWrites) * payloadBytes });
  try {
    while (performance.now() < workDeadline) {
      oldest = await oplogEdge(local, 1, workDeadline);
      if (oldest?.greaterThan(boundary)) {
        const details = await diagnostics(db, deadline);
        console.info("Expiry fixture rolled oplog", JSON.stringify({ ...evidence(), details }));
        return;
      }
      if (extraWrites < extraWriteLimit) {
        // A checkpoint advances the pin, but only a later marker wakes the truncator promptly.
        await db.admin().command({ fsync: 1 }, { timeoutMS: remaining(workDeadline) });
        remaining(workDeadline);
        const payload = randomBytes(payloadBytes);
        extraWrites++;
        await db.collection("noise").insertOne({ payload }, { timeoutMS: remaining(workDeadline) });
      }
      await delay(Math.min(paceMs, remaining(workDeadline)));
    }
  } catch (error: unknown) {
    const details = await diagnostics(db, deadline);
    throw new Error(`Expiry fixture failed: ${JSON.stringify({ ...evidence(), details })}`, { cause: error });
  }
  const details = await diagnostics(db, deadline);
  throw new Error(`Expiry fixture did not roll oplog: ${JSON.stringify({ ...evidence(), details })}`);
}

/** Deliberately rolls a dedicated 1MB test oplog, never the normal test service. */
export async function expiredMongo() {
  const uri = process.env.TEST_EXPIRY_MONGODB_URI ?? "mongodb://127.0.0.1:27188/?directConnection=true&replicaSet=rs0";
  const env = await mongo(uri);
  const source = new ChangeStreamSource(env.config);
  try {
    const local = env.client.db("local");
    const stats: unknown = await local.command({ collStats: "oplog.rs" });
    const size = record(stats).maxSize;
    if (typeof size !== "number" || !Number.isFinite(size) || size <= 0 || size > 4 * 1024 * 1024) {
      throw new Error("Expiry test requires dedicated <=4MB oplog");
    }
    const parameters: unknown = await env.db.admin().command({ getParameter: 1, syncdelay: 1 });
    if (record(parameters).syncdelay !== 1) throw new Error("Start dedicated expiry Mongo with --syncdelay 1");
    const history = new MongoHistory(env.config);
    const before = await history.boundary();
    await env.db.collection("commits").insertOne(commit(10));
    const cursor = source.watch({ kind: "boundary", mode: "changeStream", ts: before })[Symbol.asyncIterator]();
    const first = await cursor.next();
    if (first.done) throw new Error("No initial token");
    await cursor.return?.();
    const boundary = await history.boundary();
    await env.db.collection("commits").insertOne(commit(20));
    const started = performance.now();
    const initialDeadline = started + 5000;
    for (let i = 0; i < initialWrites; i++) {
      await env.db.collection("noise").insertOne({ payload: randomBytes(payloadBytes) },
        { timeoutMS: remaining(initialDeadline) });
    }
    await rollOplog(env.db, boundary, performance.now() - started);
    return { ...env, primary: first.value.position, boundary };
  } catch (error: unknown) { await env.close(); throw error; }
  finally { await source.close(); }
}
