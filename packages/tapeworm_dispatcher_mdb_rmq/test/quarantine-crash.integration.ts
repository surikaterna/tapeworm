import { fork, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { resolve } from "node:path";
import { ObjectId } from "mongodb";
import { expect, test } from "vitest";
import { CommitPublisher, MongoResumeTokenStore, QuarantineService } from "../index";
import { record, text } from "../src/validation";
import { quarantineFixture } from "./quarantine-integration-fixture";
import { request } from "./quarantine-fixtures";
import { eventually, mongoUri, rabbit, rabbitUri } from "./services";
import { state } from "./fixtures";

function confirmed(child: ChildProcess): Promise<string> {
  return new Promise((resolveToken, reject) => {
    const cleanup = () => { clearTimeout(timer); child.removeListener("message", receive); child.removeListener("exit", exit); };
    const receive = (value: unknown) => {
      cleanup();
      const data = record(value);
      if (data.kind === "confirmed-before-completion") resolveToken(text(data.token));
      else reject(new Error("Child failed before confirm barrier"));
    };
    const exit = () => { cleanup(); reject(new Error("Child exited before confirm barrier")); };
    const timer = setTimeout(() => { cleanup(); reject(new Error("Confirm barrier timed out")); }, 10000);
    child.on("message", receive); child.once("exit", exit);
  });
}
async function kill(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit"); child.kill("SIGKILL"); await exited;
}
test.each(["changeStream", "oplog"] as const)("confirmed redrive SIGKILL before completion is manually retryable in %s", async (mode) => {
  const mq = await rabbit();
  const f = await quarantineFixture(1500, mq.exchange, mode);
  const publisher = new CommitPublisher(f.config.rabbitmq);
  const service = new QuarantineService({ ...f.config, store: f.store, source: f.source, publisher,
    sourceRetention: "immutable-until-resolved" });
  const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
  await checkpoints.save(state()); const before = await checkpoints.load();
  const child = fork(resolve("dist-worker/test/quarantine-worker.js"), [], { stdio: ["ignore", "inherit", "inherit", "ipc"],
    env: { ...process.env, TEST_MONGODB_URI: mongoUri, TEST_RABBITMQ_URI: rabbitUri, TEST_MODE: mode,
      TEST_DATABASE: f.mongodb.db.databaseName, TEST_EXCHANGE: mq.exchange, TEST_QUARANTINE_ID: f.captured.id } });
  try {
    const token = await confirmed(child);
    const first = await mq.channel.get(mq.queue, { noAck: true });
    if (!first) throw new Error("Confirm barrier lacked broker publication");
    expect(record(first.properties).messageId).toBe(f.value.id);
    expect((await f.store.find(f.reference))?.status).toBe("claimed");
    await kill(child);
    await eventually(async () => Boolean(await f.mongodb.db.collection("quarantine").findOne({ _id: new ObjectId(f.captured.id),
      $expr: { $lte: ["$claimExpiresAt", "$$NOW"] } })));
    expect(await service.redrive(f.captured.id, request)).toEqual({ kind: "published" });
    expect(await f.store.finish(f.captured.id, token, "published")).toBe(false);
    const second = await mq.channel.get(mq.queue, { noAck: true });
    if (!second) throw new Error("Missing manual retry duplicate");
    expect(second.content).toEqual(first.content); expect(record(second.properties).messageId).toBe(f.value.id);
    expect((await f.store.find(f.reference))?.attempts.map((attempt) => attempt.result)).toEqual(["outcome-unknown", "published"]);
    expect(await checkpoints.load()).toEqual(before);
  } finally { await kill(child); await service.close(); await publisher.close(); await f.mongodb.close(); await mq.close(); }
});
