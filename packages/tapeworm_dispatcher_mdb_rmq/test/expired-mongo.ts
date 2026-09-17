import { randomBytes } from "node:crypto";
import { eventually, mongo } from "./services";
import { MongoHistory } from "../src/history";
import { ChangeStreamSource } from "../src/live-source";
import { commit } from "./fixtures";
import { record, timestamp } from "../src/validation";

/** Deliberately rolls a dedicated 1MB test oplog, never the normal test service. */
export async function expiredMongo() {
  const uri = process.env.TEST_EXPIRY_MONGODB_URI ?? "mongodb://127.0.0.1:27188/?directConnection=true&replicaSet=rs0";
  const env = await mongo(uri);
  const source = new ChangeStreamSource(env.config);
  try {
    const local = env.client.db("local");
    const stats: unknown = await local.command({ collStats: "oplog.rs" });
    const size = record(stats).maxSize;
    if (typeof size !== "number" || size > 4 * 1024 * 1024) throw new Error("Expiry test requires dedicated <=4MB oplog");
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
    for (let i = 0; i < 80; i++) {
      await env.db.collection("noise").insertOne({ payload: randomBytes(512 * 1024) });
    }
    await env.db.admin().command({ fsync: 1 });
    await eventually(async () => {
      const oldest = await local.collection<Record<string, unknown>>("oplog.rs").findOne({}, { sort: { $natural: 1 } });
      return !!oldest && timestamp(oldest.ts).greaterThan(boundary);
    }, 20000);
    return { ...env, primary: first.value.position, boundary };
  } catch (error: unknown) { await env.close(); throw error; }
  finally { await source.close(); }
}
