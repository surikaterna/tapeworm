import { fork, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { resolve } from "node:path";
import { expect, test } from "vitest";
import { mongo, rabbit, mongoUri, rabbitUri } from "./services";
import { commit } from "./fixtures";
import { MongoResumeTokenStore } from "../index";
import { record } from "../src/validation";

function message(child: ChildProcess, expected: string): Promise<void> {
  return new Promise((resolveMessage, reject) => {
    const cleanup = () => { clearTimeout(timer); child.removeListener("message", receive); child.removeListener("exit", exit); };
    const receive = (value: unknown) => {
      if (value === expected) { cleanup(); resolveMessage(); }
      if (value && typeof value === "object") { cleanup(); reject(new Error(JSON.stringify(value))); }
    };
    const exit = () => { cleanup(); reject(new Error("Child exited before expected message")); };
    const timer = setTimeout(() => { cleanup(); reject(new Error(`Waiting for ${expected}`)); }, 15000);
    child.on("message", receive); child.once("exit", exit);
  });
}

async function kill(child: ChildProcess, signal: NodeJS.Signals): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill(signal);
  await exited;
}

test.each(["changeStream", "oplog"])("SIGKILL after broker confirm before checkpoint duplicates safely in %s", async (mode) => {
  const mongodb = await mongo();
  const mq = await rabbit();
  const children: ChildProcess[] = [];
  const spawn = (crash: boolean) => {
    const child = fork(resolve("dist-worker/test/worker.js"), [], { stdio: ["ignore", "inherit", "inherit", "ipc"],
      env: { ...process.env, TEST_MONGODB_URI: mongoUri, TEST_RABBITMQ_URI: rabbitUri,
        TEST_DATABASE: mongodb.db.databaseName, TEST_EXCHANGE: mq.exchange, TEST_MODE: mode, TEST_CRASH: String(crash) } });
    children.push(child); return child;
  };
  try {
    const first = spawn(true);
    await message(first, "boundary-saved");
    const blocked = message(first, "confirmed-before-save");
    await mongodb.db.collection("commits").insertOne(commit(11));
    await blocked;
    const store = new MongoResumeTokenStore(mongodb.db, "checkpoint");
    expect((await store.load())?.lastCommitToken).toBeUndefined();
    await kill(first, "SIGKILL");
    const second = spawn(false);
    await message(second, "saved");
    await kill(second, "SIGTERM");
    const ids: unknown[] = [];
    for (let i = 0; i < 2; i++) {
      const value = await mq.channel.get(mq.queue, { noAck: true });
      if (!value) throw new Error("Missing duplicate publication");
      ids.push(record(value.properties).messageId);
    }
    expect(ids).toEqual(["commit-11", "commit-11"]);
    expect((await store.load())?.lastCommitToken).toBeDefined();
  } finally {
    await Promise.all(children.map((child) => kill(child, "SIGKILL")));
    await mongodb.close(); await mq.close();
  }
});
