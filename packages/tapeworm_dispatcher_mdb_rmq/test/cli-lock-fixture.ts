import { checkpointFeed, MongoResumeTokenStore } from "../index";
import { MongoHistory } from "../src/history";
import { record } from "../src/validation";
import { commit } from "./fixtures";
import { mongo, rabbit, rabbitUri, eventually } from "./services";
import type { Db } from "mongodb";

export async function checkpointOperations(db: Db): Promise<Record<string, unknown>[]> {
  const result: unknown = await db.admin().command({ currentOp: 1, ns: `${db.databaseName}.checkpoint`, op: "update" });
  const values: unknown = record(result).inprog;
  if (!Array.isArray(values)) throw new Error("Missing currentOp results");
  const operations: unknown[] = values;
  return operations.map(record);
}

export async function lockedCheckpoint() {
  const mongodb = await mongo();
  const mq = await rabbit();
  const store = new MongoResumeTokenStore(mongodb.db, "checkpoint", { checkpointKey: "shutdown-test" });
  const config = { mongodb: mongodb.config, rabbitmq: { uri: rabbitUri, exchange: mq.exchange }, feedId: "shutdown-test" };
  const boundary = await new MongoHistory(mongodb.config).boundary();
  await store.save({ version: 1, feed: checkpointFeed(config), updatedAt: new Date(),
    primary: { kind: "boundary", mode: "changeStream", ts: boundary } });
  await mongodb.db.collection("commits").insertOne(commit(11), { writeConcern: { w: "majority" } });
  return { mongodb, mq, store,
    waitBlocked: () => eventually(async () => (await checkpointOperations(mongodb.db)).some((op) => op.waitingForLock === true)),
    waitIdle: () => eventually(async () => (await checkpointOperations(mongodb.db)).length === 0),
  };
}

export async function messageIds(mq: Awaited<ReturnType<typeof rabbit>>): Promise<string[]> {
  const ids: string[] = [];
  let message = await mq.channel.get(mq.queue, { noAck: true });
  while (message) {
    const body: unknown = JSON.parse(message.content.toString());
    const id: unknown = record(message.properties).messageId;
    if (typeof id !== "string" || record(body).id !== id) throw new Error("Unstable commit message identity");
    ids.push(id);
    message = await mq.channel.get(mq.queue, { noAck: true });
  }
  return ids;
}
