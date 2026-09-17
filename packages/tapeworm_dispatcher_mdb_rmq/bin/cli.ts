#!/usr/bin/env node
import debug from "debug";
import { LoggerFactory } from "slf";
import slfDebug from "slf-debug";
import { MongoClient } from "mongodb";
import { Dispatcher } from "../src/dispatcher";
import { MongoResumeTokenStore } from "../src/resume/mongodb-store";
import { parseArgs } from "./args";
import { runCli } from "./lifecycle";

debug.enable(process.env.DEBUG || "tapeworm-dispatcher:*");
LoggerFactory.setFactory(slfDebug);

function observe(dispatcher: Dispatcher): void {
  let scanned = 0;
  dispatcher.on("started", () => { console.info("Dispatcher started; externally fenced single owner required"); });
  dispatcher.on("error", (error) => { console.error("Dispatcher retry:", error.message); });
  dispatcher.on("fatal", (error) => { console.error("Dispatcher fatal:", error.message); });
  dispatcher.on("recovery", (event) => {
    if (event.phase === "scan" && ++scanned % 1000 !== 0) return;
    console.warn("CDC recovery", event);
  });
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);
  if (!args.checkpointKey || !args.feedId) console.warn("Legacy checkpoint default: unsafe across feeds/clusters. Set --checkpoint-key and --feed-id.");
  const client = new MongoClient(args.mongodbUri);
  await runCli(client, async () => {
    await client.connect();
    const db = client.db(args.database);
    const dispatcher = new Dispatcher({
      mongodb: { db, collection: args.collection }, rabbitmq: { uri: args.rabbitmqUri, exchange: args.exchange },
      resumeTokenStore: new MongoResumeTokenStore(db, args.resumeCollection, { checkpointKey: args.checkpointKey }),
      tenant: args.tenant, watchMode: args.watchMode, feedId: args.feedId,
      adoptLegacyCheckpoint: args.adoptLegacyCheckpoint,
    });
    observe(dispatcher);
    return dispatcher;
  }, process, {
    onDeadline: (error) => { console.error(error.message); process.exit(124); },
    onLateError: (error) => { console.error(error instanceof Error ? error.message : "Late CLI failure"); },
  });
}

void main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : "Dispatcher failed");
  process.exitCode = 1;
});
