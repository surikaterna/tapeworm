#!/usr/bin/env node

import debug from "debug";
import { LoggerFactory } from "slf";
import slfDebug from "slf-debug";

// Wire slf-debug as the logging backend
// Use DEBUG env var to control output (e.g. DEBUG=tapeworm-dispatcher:*)
debug.enable(process.env.DEBUG || "tapeworm-dispatcher:*");
LoggerFactory.setFactory(slfDebug);

import { MongoClient } from "mongodb";
import { Dispatcher } from "../src/dispatcher";
import { MongoResumeTokenStore } from "../src/resume/mongodb-store";

const LOG = LoggerFactory.getLogger("tapeworm-dispatcher:cli");

interface CliArgs {
  mongodbUri: string;
  database: string;
  collection: string;
  rabbitmqUri: string;
  exchange: string;
  resumeCollection: string;
  tenant?: string;
  watchMode: "changeStream" | "oplog";
}

function parseArgs(argv: string[]): CliArgs {
  const args: Record<string, string> = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith("--") && i + 1 < argv.length) {
      const key = arg.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      args[key] = argv[++i];
    }
  }

  // ENV var fallbacks: CLI args take precedence over ENV vars
  const envFallbacks: Record<string, string | undefined> = {
    mongodbUri: process.env.MONGODB_URI,
    database: process.env.DATABASE,
    collection: process.env.COLLECTION,
    rabbitmqUri: process.env.RABBITMQ_URI,
    exchange: process.env.EXCHANGE,
    resumeCollection: process.env.RESUME_COLLECTION,
    watchMode: process.env.WATCH_MODE,
    tenant: process.env.TENANT,
  };
  for (const [key, envVal] of Object.entries(envFallbacks)) {
    if (!args[key] && envVal) {
      args[key] = envVal;
    }
  }

  const required = [
    "mongodbUri",
    "database",
    "collection",
    "rabbitmqUri",
    "exchange",
  ];
  for (const key of required) {
    if (!args[key]) {
      console.error(
        `Missing required argument: --${key.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)}`,
      );
      console.error(
        "\nUsage: tapeworm-dispatcher \\\n" +
          "  --mongodb-uri <uri>       (env: MONGODB_URI) \\\n" +
          "  --database <name>         (env: DATABASE) \\\n" +
          "  --collection <name>       (env: COLLECTION) \\\n" +
          "  --rabbitmq-uri <amqp-uri> (env: RABBITMQ_URI) \\\n" +
          "  --exchange <name>         (env: EXCHANGE) \\\n" +
          "  [--resume-collection <name>]  (env: RESUME_COLLECTION) \\\n" +
          "  [--watch-mode <changeStream|oplog>]  (env: WATCH_MODE) \\\n" +
          "  [--tenant <tenant-id>]    (env: TENANT)\n\n" +
          "Environment-only:\n" +
          "  DEBUG=tapeworm-dispatcher:*  (controls log output)\n\n" +
          "CLI arguments take precedence over environment variables.",
      );
      process.exit(1);
    }
  }

  const watchMode = (args.watchMode || "changeStream") as
    | "changeStream"
    | "oplog";
  if (watchMode !== "changeStream" && watchMode !== "oplog") {
    console.error(
      `Invalid --watch-mode: ${watchMode}. Must be "changeStream" or "oplog".`,
    );
    process.exit(1);
  }

  return {
    mongodbUri: args.mongodbUri,
    database: args.database,
    collection: args.collection,
    rabbitmqUri: args.rabbitmqUri,
    exchange: args.exchange,
    resumeCollection: args.resumeCollection || "tw_dispatcher_state",
    tenant: args.tenant,
    watchMode,
  };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);

  const client = new MongoClient(args.mongodbUri);
  await client.connect();
  const db = client.db(args.database);

  const resumeTokenStore = new MongoResumeTokenStore(db, args.resumeCollection);

  const dispatcher = new Dispatcher({
    mongodb: {
      db,
      collection: args.collection,
    },
    rabbitmq: {
      uri: args.rabbitmqUri,
      exchange: args.exchange,
    },
    resumeTokenStore,
    tenant: args.tenant,
    watchMode: args.watchMode,
  });

  dispatcher.on("started", () => {
    LOG.info(
      "dispatcher started watchMode=%s collection=%s",
      args.watchMode,
      args.collection,
    );
  });
  dispatcher.on("dispatched", (commit) => {
    LOG.debug(
      "commit dispatched commitId=%s streamId=%s",
      commit.id,
      commit.streamId,
    );
  });
  dispatcher.on("resumed", (state) => {
    LOG.info("resumed from token %s", state.lastCommitToken ?? "none");
  });
  dispatcher.on("fallback", () => {
    LOG.warn("change stream token expired, falling back to .token cursor");
  });
  dispatcher.on("error", (err) => {
    LOG.error("dispatcher error: %s", err.message);
  });
  dispatcher.on("fatal", (err) => {
    LOG.error("fatal dispatcher error: %s", err.message);
    process.exit(1);
  });

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

    try {
      await dispatcher.stop();
    } catch (err: any) {
      LOG.error("error during dispatcher stop: %s", err.message);
    }
    try {
      await client.close();
    } catch (err: any) {
      LOG.error("error during mongodb close: %s", err.message);
    }

    LOG.info("shutdown complete");
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  process.on("uncaughtException", (err) => {
    LOG.error("uncaught exception: %s stack=%s", err.message, err.stack);
    process.exit(1);
  });
  process.on("unhandledRejection", (reason) => {
    LOG.error("unhandled rejection: %s", String(reason));
    process.exit(1);
  });

  await dispatcher.start();
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
