import { MongoClient } from "mongodb";
import { CommitPublisher, MongoQuarantineStore, MongoQuarantineSourceReader, QuarantineService, checkpointFeed } from "../../../index";
import type { AttemptResult } from "../../../index";
import { text } from "../../../src/validation";

class BarrierStore extends MongoQuarantineStore {
  override async finish(_id: string, token: string, result: AttemptResult): Promise<boolean> {
    if (result !== "published") throw new Error("Worker publication failed");
    process.send?.({ kind: "confirmed-before-completion", token });
    return new Promise<boolean>(() => {});
  }
}
async function run(): Promise<void> {
  const client = new MongoClient(text(process.env.TEST_MONGODB_URI));
  await client.connect();
  const db = client.db(text(process.env.TEST_DATABASE));
  const config = { mongodb: { db, collection: "commits" },
    rabbitmq: { uri: text(process.env.TEST_RABBITMQ_URI), exchange: text(process.env.TEST_EXCHANGE) },
    watchMode: process.env.TEST_MODE === "oplog" ? "oplog" as const : "changeStream" as const };
  const feed = checkpointFeed(config);
  const store = new BarrierStore(db, "quarantine", { feed, sourceCollection: "commits", leaseMs: 1500 });
  const publisher = new CommitPublisher(config.rabbitmq);
  const service = new QuarantineService({ ...config, store, publisher, sourceRetention: "immutable-until-resolved",
    source: new MongoQuarantineSourceReader(db, "commits", feed) });
  try { await service.redrive(text(process.env.TEST_QUARANTINE_ID), { actor: "crash-worker", reason: "repair" }); }
  finally { await service.close(); await publisher.close(); await client.close(); }
}
void run().catch(() => { process.send?.({ kind: "worker-failed" }); process.exitCode = 1; });
