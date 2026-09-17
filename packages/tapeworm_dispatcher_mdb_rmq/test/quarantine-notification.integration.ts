import { expect, test } from "vitest";
import { DeliveryHalted, Dispatcher, MongoResumeTokenStore, QuarantineFailureHandler } from "../index";
import { quarantineFixture } from "./quarantine-integration-fixture";
import { mixedPolicy, oversizedCommit } from "./quarantine-fixtures";
import { commit, token } from "./fixtures";
import { eventually, rabbit } from "./services";
import { record } from "../src/validation";

test.each(["changeStream", "oplog"] as const)("%s post-checkpoint observer failure halts; new dispatcher resumes after saved poison", async (mode) => {
  const mq = await rabbit(); const f = await quarantineFixture(60000, mq.exchange, mode);
  const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
  const adapter = new QuarantineFailureHandler({ ...f.config,
    quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved" } });
  const config = { ...f.config, resumeTokenStore: checkpoints, publication: mixedPolicy, failureHandler: adapter };
  const dispatcher = new Dispatcher(config); const cause = new Error("Observer failed");
  let notifications = 0; let retries = 0; const dispatched: string[] = [];
  adapter.on("quarantined", () => { notifications++; throw cause; });
  dispatcher.on("error", () => { retries++; }); dispatcher.on("dispatched", (value) => { dispatched.push(value.id); });
  const outcome = dispatcher.start().catch((error: unknown) => error);
  let restarted: Dispatcher | undefined; let restartOutcome: Promise<unknown> | undefined;
  try {
    await eventually(async () => Boolean((await checkpoints.load())?.primary));
    await f.mongodb.db.collection("commits").insertOne(oversizedCommit(2));
    await f.mongodb.db.collection("commits").insertOne(commit(3));
    const failure = await outcome;
    expect(failure).toBeInstanceOf(DeliveryHalted); expect(failure).toHaveProperty("cause", cause);
    expect((await checkpoints.load())?.lastCommitToken).toBe(token(2));
    expect(notifications).toBe(1); expect(retries).toBe(0); expect(dispatched).toEqual([]);
    expect(await mq.channel.get(mq.queue, { noAck: true })).toBe(false);
    restarted = new Dispatcher(config); restarted.on("error", () => {});
    restarted.on("dispatched", (value) => { dispatched.push(value.id); });
    restartOutcome = restarted.start().catch((error: unknown) => error);
    await eventually(async () => (await checkpoints.load())?.lastCommitToken === token(3));
    expect(notifications).toBe(1); expect(dispatched).toEqual(["commit-3"]);
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing next healthy publication");
    expect(record(message.properties).messageId).toBe("commit-3");
    const poison = (await f.store.list()).records.find((value) => value.reference.commitId === "commit-2");
    expect(poison?.observations).toBe(1);
  } finally {
    await dispatcher.stop(); await outcome; await restarted?.stop(); await restartOutcome;
    await f.mongodb.close(); await mq.close();
  }
});
