import { expect, test } from "vitest";
import { Dispatcher, MongoResumeTokenStore, QuarantinePaused } from "../index";
import type { QuarantinedEvent, QuarantineConfig } from "../index";
import { quarantineFixture } from "./quarantine-integration-fixture";
import { eventually, rabbit } from "./services";
import { commit, token } from "./fixtures";
import { record } from "../src/validation";

test.each(["changeStream", "oplog"] as const)("dispatcher pause is terminal and emits no dispatched success in %s", async (mode) => {
  const mq = await rabbit();
  const f = await quarantineFixture(60000, mq.exchange, mode);
  const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
  const dispatcher = new Dispatcher({ ...f.config, resumeTokenStore: checkpoints,
    quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved", mode: "pause" },
    publication: { maxMessageBytes: 1 } });
  const events: QuarantinedEvent[] = [];
  let dispatched = 0;
  dispatcher.on("quarantined", (event) => { events.push(event); });
  dispatcher.on("dispatched", () => { dispatched++; }); dispatcher.on("error", () => {});
  const running = dispatcher.start(); const outcome = running.catch((error: unknown) => error);
  try {
    await eventually(async () => Boolean((await checkpoints.load())?.primary));
    await f.mongodb.db.collection("commits").insertOne(commit(2));
    await f.mongodb.db.collection("commits").insertOne(commit(3));
    expect(await outcome).toBeInstanceOf(QuarantinePaused);
    expect(events).toHaveLength(1); expect(events[0]).toMatchObject({ checkpointAdvanced: false, resolution: "unresolved" });
    expect(dispatched).toBe(0); expect((await checkpoints.load())?.lastCommitToken).toBeUndefined();
    expect(await mq.channel.get(mq.queue, { noAck: true })).toBe(false);
  } finally { await dispatcher.stop(); await outcome; await f.mongodb.close(); await mq.close(); }
});
test.each([["changeStream", undefined], ["oplog", undefined], ["changeStream", "continue"], ["oplog", "continue"]] as const)(
  "dispatcher %s with enabled quarantine mode %s delivers the next healthy Rabbit message", async (mode, quarantineMode) => {
  const mq = await rabbit();
  const f = await quarantineFixture(60000, mq.exchange, mode);
  const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
  const quarantine: QuarantineConfig = { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved",
    ...(quarantineMode ? { mode: quarantineMode } : {}) };
  const dispatcher = new Dispatcher({ ...f.config, resumeTokenStore: checkpoints, quarantine, publication: {
    validateRecord: (value) => value.id === "commit-2" ? { kind: "reject", code: "unsupported-schema" } : { kind: "allow" } } });
  const events: QuarantinedEvent[] = []; const dispatched: string[] = [];
  dispatcher.on("quarantined", (event) => { events.push(event); });
  dispatcher.on("dispatched", (value) => { dispatched.push(value.id); }); dispatcher.on("error", () => {});
  const running = dispatcher.start(); const outcome = running.catch((error: unknown) => error);
  try {
    await eventually(async () => Boolean((await checkpoints.load())?.primary));
    await f.mongodb.db.collection("commits").insertOne(commit(2));
    await f.mongodb.db.collection("commits").insertOne(commit(3));
    await eventually(async () => (await checkpoints.load())?.lastCommitToken === token(3));
    expect(quarantine).not.toHaveProperty("acceptOrderingGaps");
    if (quarantineMode === undefined) expect(quarantine).not.toHaveProperty("mode");
    expect(events).toHaveLength(1); expect(events[0]?.checkpointAdvanced).toBe(true);
    expect(dispatched).toEqual(["commit-3"]);
    expect((await f.store.list()).records.map((item) => item.reference.commitId).sort()).toEqual(["commit-1", "commit-2"]);
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing following healthy publication");
    expect(record(message.properties).messageId).toBe("commit-3");
    const source: unknown = await f.mongodb.db.collection("commits").findOne({ id: "commit-3" });
    expect(message.content.toString()).toBe(JSON.stringify(source));
    expect(await mq.channel.get(mq.queue, { noAck: true })).toBe(false);
    expect((await f.store.list({ status: "quarantined" })).records).toHaveLength(2);
  } finally { await dispatcher.stop(); await outcome; await f.mongodb.close(); await mq.close(); }
});
