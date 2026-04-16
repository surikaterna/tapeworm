#!/usr/bin/env node

import { MongoClient } from "mongodb";
import { Dispatcher } from "../lib/dispatcher";
import { MongoResumeTokenStore } from "../lib/resume/mongodb-store";

interface CliArgs {
  mongodbUri: string;
  database: string;
  collection: string;
  rabbitmqUri: string;
  exchange: string;
  resumeCollection: string;
  tenant?: string;
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
          "  --mongodb-uri <uri> \\\n" +
          "  --database <name> \\\n" +
          "  --collection <commits-collection> \\\n" +
          "  --rabbitmq-uri <amqp-uri> \\\n" +
          "  --exchange <exchange-name> \\\n" +
          "  [--resume-collection <name>] \\\n" +
          "  [--tenant <tenant-id>]",
      );
      process.exit(1);
    }
  }

  return {
    mongodbUri: args.mongodbUri,
    database: args.database,
    collection: args.collection,
    rabbitmqUri: args.rabbitmqUri,
    exchange: args.exchange,
    resumeCollection: args.resumeCollection || "tw_dispatcher_state",
    tenant: args.tenant,
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
  });

  dispatcher.on("started", () => {
    console.log(`Dispatcher started — watching ${args.collection}`);
  });
  dispatcher.on("dispatched", (commit) => {
    console.log(`Dispatched commit ${commit.id} (stream: ${commit.streamId})`);
  });
  dispatcher.on("resumed", (state) => {
    console.log(
      `Resumed from token (last commit: ${state.lastCommitToken ?? "none"})`,
    );
  });
  dispatcher.on("fallback", () => {
    console.warn("Change stream token expired — falling back to .token cursor");
  });
  dispatcher.on("error", (err) => {
    console.error("Dispatcher error:", err.message);
  });
  dispatcher.on("fatal", (err) => {
    console.error("Fatal dispatcher error:", err.message);
    process.exit(1);
  });

  const shutdown = async () => {
    console.log("\nShutting down...");
    await dispatcher.stop();
    await client.close();
    console.log("Shutdown complete.");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  await dispatcher.start();
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
