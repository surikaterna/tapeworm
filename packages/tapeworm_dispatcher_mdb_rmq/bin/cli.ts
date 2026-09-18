#!/usr/bin/env node

import debug from "debug";
import { LoggerFactory } from "slf";
import slfDebug from "slf-debug";
import { MongoClient } from "mongodb";
import { Dispatcher } from "../src/dispatcher";
import { MongoResumeTokenStore } from "../src/checkpoints/mongodb-store";
import { checkpointFeed } from "../src/checkpoints/feed";
import { parseArgs, type CliArgs } from "./args";

debug.enable(process.env.DEBUG || "tapeworm-dispatcher:*");
LoggerFactory.setFactory(slfDebug);
const LOG = LoggerFactory.getLogger("tapeworm-dispatcher:cli");

function observe(dispatcher: Dispatcher, args: CliArgs): void {
  dispatcher.on("started", () => {
    LOG.info("dispatcher started watchMode=%s collection=%s", args.watchMode, args.collection);
  });
  dispatcher.on("dispatched", (commit) => {
    LOG.debug("commit dispatched commitId=%s streamId=%s", commit.id, commit.streamId);
  });
  dispatcher.on("resumed", (state) => { LOG.info("resumed from token %s", state.lastCommitToken ?? "none"); });
  dispatcher.on("fallback", () => { LOG.warn("change stream token expired, falling back to .token cursor"); });
  dispatcher.on("error", (err) => { LOG.error("dispatcher error: %s", err.message); });
  dispatcher.on("fatal", (err) => {
    LOG.error("fatal dispatcher error: %s", err.message);
    process.exit(1);
  });
}

function installShutdown(dispatcher: Dispatcher, client: MongoClient): void {
  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      LOG.warn("forced exit signal=%s", signal);
      process.exit(1);
    }
    shuttingDown = true;
    LOG.info("shutting down signal=%s", signal);
    const forceExitTimer = setTimeout(() => {
      LOG.error("shutdown timed out, forcing exit");
      process.exit(1);
    }, 10000);
    forceExitTimer.unref();
    try { await dispatcher.stop(); }
    catch (err: unknown) { LOG.error("error during dispatcher stop: %s", err instanceof Error ? err.message : String(err)); }
    try { await client.close(); }
    catch (err: unknown) { LOG.error("error during mongodb close: %s", err instanceof Error ? err.message : String(err)); }
    LOG.info("shutdown complete");
    process.exit(0);
  };
  process.on("SIGINT", () => { void shutdown("SIGINT"); });
  process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
  process.on("uncaughtException", (err) => {
    LOG.error("uncaught exception: %s stack=%s", err.message, err.stack);
    process.exit(1);
  });
  process.on("unhandledRejection", (reason: unknown) => {
    LOG.error("unhandled rejection: %s", String(reason));
    process.exit(1);
  });
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);
  const client = new MongoClient(args.mongodbUri);
  await client.connect();
  const db = client.db(args.database);
  const config = {
    mongodb: { db, collection: args.collection },
    rabbitmq: { uri: args.rabbitmqUri, exchange: args.exchange },
    tenant: args.tenant, watchMode: args.watchMode, feedId: args.feedId,
    adoptLegacyCheckpoint: args.adoptLegacyCheckpoint,
  };
  const resumeTokenStore = new MongoResumeTokenStore(db, args.resumeCollection, {
    checkpointKey: args.checkpointKey, feedId: checkpointFeed(config),
    adoptLegacyCheckpoint: args.adoptLegacyCheckpoint,
  });
  const dispatcher = new Dispatcher({ ...config, resumeTokenStore });
  observe(dispatcher, args);
  installShutdown(dispatcher, client);
  await dispatcher.start();
}

main().catch((err: unknown) => {
  console.error("Fatal:", err);
  process.exit(1);
});
