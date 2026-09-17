import { MongoClient } from "mongodb";
import { afterEach, expect, test, vi } from "vitest";
import { Dispatcher, MongoQuarantineStore, MongoResumeTokenStore, checkpointFeed, QuarantineFailureHandler } from "../index";
import * as validation from "../src/validation";
import * as references from "../src/quarantine/validation";
import * as publication from "../src/publication-policy";
import { commit, token } from "./fixtures";
import { mixedPolicy, oversizedCommit } from "./quarantine-fixtures";
import { eventually, mongoUri, rabbit, rabbitUri, unique } from "./services";

afterEach(() => { vi.restoreAllMocks(); });
async function fixture(exchange: string, mode: "changeStream" | "oplog" = "changeStream") {
  const client = await new MongoClient(mongoUri, { monitorCommands: true }).connect();
  const db = client.db(unique());
  await db.createCollection("commits");
  await db.collection("commits").createIndex({ token: 1 });
  const config = { mongodb: { db, collection: "commits", retryDelayMs: 500, maxRetries: 20 },
    rabbitmq: { uri: rabbitUri, exchange }, watchMode: mode };
  const scope = { feed: checkpointFeed(config), sourceCollection: "commits" };
  const store = new MongoQuarantineStore(db, "quarantine", scope);
  const checkpoints = new MongoResumeTokenStore(db, "checkpoint");
  const commands: Record<string, unknown>[] = [];
  client.on("commandStarted", (event) => { const command: unknown = event.command; commands.push(validation.record(command)); });
  return { db, config, scope, store, checkpoints, commands,
    close: async () => { await db.dropDatabase(); await client.close(); } };
}
function quarantineCommands(commands: Record<string, unknown>[]) {
  return commands.filter((command) => Object.values(command).includes("quarantine"));
}
function forbidden(): never { throw new Error("Quarantine storage unavailable"); }

test.each(["changeStream", "oplog"] as const)("%s healthy startup/confirmed delivery needs no quarantine metadata or extra decoding", async (mode) => {
  const mq = await rabbit(); const f = await fixture(mq.exchange, mode);
  const spies = [vi.spyOn(f.store, "initialize"), vi.spyOn(f.store, "find"), vi.spyOn(f.store, "capture"),
    vi.spyOn(f.store, "list"), vi.spyOn(f.store, "claim"), vi.spyOn(f.store, "finish")];
  for (const spy of spies) spy.mockImplementation(forbidden);
  const decode = vi.spyOn(validation, "decodeCommit"); const encode = vi.spyOn(publication, "encodePublication");
  const reference = vi.spyOn(references, "sourceReference");
  const adapter = new QuarantineFailureHandler({ ...f.config,
    quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved" } });
  const handle = vi.spyOn(adapter, "handle");
  const dispatcher = new Dispatcher({ ...f.config, resumeTokenStore: f.checkpoints, publication: mixedPolicy, failureHandler: adapter });
  dispatcher.on("error", () => {});
  const outcome = dispatcher.start().catch((error: unknown) => error);
  try {
    await eventually(async () => Boolean((await f.checkpoints.load())?.primary));
    await f.db.collection("commits").insertOne(commit(1));
    await eventually(async () => (await f.checkpoints.load())?.lastCommitToken === token(1));
    expect(decode).toHaveBeenCalledTimes(1); expect(encode).toHaveBeenCalledTimes(1);
    expect(reference).not.toHaveBeenCalled(); expect(handle).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    expect(quarantineCommands(f.commands)).toEqual([]);
    expect(f.commands.filter((command) => command.listIndexes === "commits")).toEqual([]);
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing healthy Rabbit message");
    expect(validation.record(message.properties).messageId).toBe("commit-1");
    const source: unknown = await f.db.collection("commits").findOne({ id: "commit-1" });
    expect(message.content.toString()).toBe(JSON.stringify(source));
    expect(await f.db.listCollections({ name: "quarantine" }).toArray()).toEqual([]);
  } finally { await dispatcher.stop(); await outcome; await f.close(); await mq.close(); }
});

test("first oversized record initializes source then store before capture; two poisons reuse readiness", async () => {
  const mq = await rabbit(); const f = await fixture(mq.exchange);
  await f.db.collection("commits").createIndex({ id: 1 }, { unique: true });
  f.commands.length = 0;
  const capture = vi.spyOn(f.store, "capture");
  const adapter = new QuarantineFailureHandler({ ...f.config,
    quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved" } });
  const dispatcher = new Dispatcher({ ...f.config, resumeTokenStore: f.checkpoints, publication: mixedPolicy, failureHandler: adapter });
  dispatcher.on("error", () => {});
  const outcome = dispatcher.start().catch((error: unknown) => error);
  try {
    await eventually(async () => Boolean((await f.checkpoints.load())?.primary));
    expect(quarantineCommands(f.commands)).toEqual([]);
    await f.db.collection("commits").insertMany([oversizedCommit(1), oversizedCommit(2), commit(3)]);
    await eventually(async () => (await f.checkpoints.load())?.lastCommitToken === token(3));
    expect(capture).toHaveBeenCalledTimes(2);
    expect(f.commands.filter((command) => command.listIndexes === "commits")).toHaveLength(1);
    expect(f.commands.filter((command) => command.listIndexes === "quarantine")).toHaveLength(1);
    expect(f.commands.filter((command) => command.createIndexes === "quarantine")).toHaveLength(3);
    const sourceReady = f.commands.findIndex((command) => command.listIndexes === "commits");
    const storeReady = f.commands.findIndex((command) => command.listIndexes === "quarantine");
    const captureIndex = f.commands.findIndex((command) => command.update === "quarantine");
    expect(sourceReady).toBeLessThan(storeReady); expect(storeReady).toBeLessThan(captureIndex);
    expect((await f.store.list()).records).toHaveLength(2);
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing next healthy message");
    expect(validation.record(message.properties).messageId).toBe("commit-3");
  } finally { await dispatcher.stop(); await outcome; await f.close(); await mq.close(); }
});

async function breakReadiness(f: Awaited<ReturnType<typeof fixture>>, failure: string) {
  if (failure === "source-index") return;
  await f.db.collection("commits").createIndex({ id: 1 }, { unique: true });
  if (failure === "store-index") {
    await f.db.createCollection("quarantine", { collation: { locale: "en", strength: 1 } });
    await f.db.collection("quarantine").createIndex({ feed: 1 }, { name: "legacy_unique", unique: true });
  } else vi.spyOn(f.store, "initialize").mockRejectedValueOnce(new Error("Storage unavailable"));
}
async function repairReadiness(f: Awaited<ReturnType<typeof fixture>>, failure: string) {
  if (failure === "source-index") await f.db.collection("commits").createIndex({ id: 1 }, { unique: true });
  if (failure === "store-index") await f.db.collection("quarantine").dropIndex("legacy_unique");
}
test.each(["source-index", "store-index", "storage"])("cold %s failure blocks capture/checkpoint; repair retries cached readiness", async (failure) => {
  const mq = await rabbit(); const f = await fixture(mq.exchange);
  await breakReadiness(f, failure); f.commands.length = 0;
  const capture = vi.spyOn(f.store, "capture");
  const adapter = new QuarantineFailureHandler({ ...f.config,
    quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved" } });
  const dispatcher = new Dispatcher({ ...f.config, resumeTokenStore: f.checkpoints, publication: mixedPolicy, failureHandler: adapter });
  let failed: () => void = () => {};
  const firstFailure = new Promise<void>((resolve) => { failed = resolve; });
  dispatcher.on("error", failed);
  const outcome = dispatcher.start().catch((error: unknown) => error);
  try {
    await eventually(async () => Boolean((await f.checkpoints.load())?.primary));
    await f.db.collection("commits").insertOne(commit(1));
    await eventually(async () => (await f.checkpoints.load())?.lastCommitToken === token(1));
    expect(quarantineCommands(f.commands)).toEqual([]);
    await f.db.collection("commits").insertOne(oversizedCommit(2)); await firstFailure;
    expect(capture).not.toHaveBeenCalled();
    expect((await f.checkpoints.load())?.lastCommitToken).toBe(token(1));
    expect(f.commands.filter((command) => command.update === "quarantine")).toEqual([]);
    await repairReadiness(f, failure);
    await eventually(async () => (await f.checkpoints.load())?.lastCommitToken === token(2));
    expect(capture).toHaveBeenCalledTimes(1);
    expect(f.commands.filter((command) => command.listIndexes === "commits").length).toBeGreaterThanOrEqual(2);
    expect((await f.store.list()).records).toHaveLength(1);
  } finally { await dispatcher.stop(); await outcome; await f.close(); await mq.close(); }
});
