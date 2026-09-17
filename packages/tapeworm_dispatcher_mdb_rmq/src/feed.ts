import type { DispatcherConfig } from "./types";

/** Public helper for matching the Mongo store's optional feed assertion to a dispatcher. */
export function checkpointFeed(config: Pick<DispatcherConfig, "feedId" | "mongodb" | "rabbitmq" | "watchMode" | "tenant">): string {
  return JSON.stringify([config.feedId ?? "default-source", config.mongodb.db.databaseName,
    config.mongodb.collection, config.watchMode ?? "changeStream", config.rabbitmq.exchange, config.tenant ?? null]);
}
