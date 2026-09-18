import { MongoClient } from "mongodb";
import { checkpointFeed } from "../../../src/checkpoints/feed";
import { QuarantineFailureHandler } from "../../../src/quarantine/failure-handler";
import type { QuarantineConfig } from "../../../src/quarantine/types";

export const base = { mongodb: { db: new MongoClient("mongodb://localhost").db("unit"), collection: "commits" },
  rabbitmq: { uri: "amqp://localhost", exchange: "unit" } };
export const handlerScope = { feed: checkpointFeed(base), sourceCollection: "commits" };
export function handler(quarantine: QuarantineConfig): QuarantineFailureHandler {
  return new QuarantineFailureHandler({ ...base, quarantine });
}
