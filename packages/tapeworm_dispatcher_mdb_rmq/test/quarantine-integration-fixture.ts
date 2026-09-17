import { checkpointFeed, MongoQuarantineSourceReader, MongoQuarantineStore } from "../index";
import { sourceReference } from "../src/quarantine/validation";
import { commit } from "./fixtures";
import { mongo, rabbitUri } from "./services";

export async function quarantineFixture(leaseMs = 60000, exchange = "quarantine-test", mode: "changeStream" | "oplog" = "changeStream") {
  const mongodb = await mongo();
  await mongodb.db.collection("commits").createIndex({ id: 1 }, { unique: true });
  const config = { mongodb: mongodb.config, rabbitmq: { uri: rabbitUri, exchange }, watchMode: mode };
  const scope = { feed: checkpointFeed(config), sourceCollection: "commits" };
  const store = new MongoQuarantineStore(mongodb.db, "quarantine", { ...scope, leaseMs });
  const source = new MongoQuarantineSourceReader(mongodb.db, "commits", scope.feed);
  await store.initialize(); await source.initialize();
  const value = commit(1);
  await mongodb.db.collection("commits").insertOne(value);
  const reference = sourceReference(value, scope);
  const captured = await store.capture(reference, "unsupported-schema");
  return { mongodb, config, scope, store, source, value, reference, captured };
}
