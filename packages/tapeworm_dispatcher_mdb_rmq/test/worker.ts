import { MongoClient } from "mongodb";
import { Dispatcher, MongoResumeTokenStore } from "../index";
import type { IResumeTokenStore, ResumeState } from "../index";

function required(key: string): string {
  const value = process.env[key];
  if (!value) throw new Error(`Missing ${key}`);
  return value;
}

async function main(): Promise<void> {
  const client = new MongoClient(required("TEST_MONGODB_URI"));
  await client.connect();
  const db = client.db(required("TEST_DATABASE"));
  const durable = new MongoResumeTokenStore(db, "checkpoint");
  const store: IResumeTokenStore = { load: () => durable.load(), save: async (state: ResumeState) => {
    if (state.lastCommitToken && process.env.TEST_CRASH === "true") {
      process.send?.("confirmed-before-save");
      await new Promise<void>(() => {});
    }
    await durable.save(state);
    process.send?.(state.lastCommitToken ? "saved" : "boundary-saved");
  } };
  const mode = required("TEST_MODE");
  if (mode !== "changeStream" && mode !== "oplog") throw new Error("Invalid mode");
  const dispatcher = new Dispatcher({ mongodb: { db, collection: "commits", retryDelayMs: 10 },
    rabbitmq: { uri: required("TEST_RABBITMQ_URI"), exchange: required("TEST_EXCHANGE") },
    resumeTokenStore: store, watchMode: mode, feedId: "crash-test" });
  dispatcher.on("error", (error) => { process.send?.({ error: error.message }); });
  const shutdown = () => { void dispatcher.stop(); };
  process.once("SIGTERM", shutdown);
  try { await dispatcher.start(); }
  finally { process.removeListener("SIGTERM", shutdown); await client.close(); process.disconnect?.(); }
}

void main().catch((error: unknown) => {
  process.send?.({ error: error instanceof Error ? error.message : "Worker failed" });
  process.exitCode = 1;
  process.disconnect?.();
});
