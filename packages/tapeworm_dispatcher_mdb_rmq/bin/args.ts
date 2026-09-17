export interface CliArgs {
  mongodbUri: string; database: string; collection: string; rabbitmqUri: string;
  exchange: string; resumeCollection: string; tenant?: string;
  watchMode: "changeStream" | "oplog"; checkpointKey?: string; feedId?: string;
  adoptLegacyCheckpoint: boolean;
}

export function parseArgs(argv: string[]): CliArgs {
  const args: Record<string, string | undefined> = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg?.startsWith("--")) args[arg.slice(2)] = argv[++i];
  }
  const get = (key: string) => args[key] ?? process.env[key.replaceAll("-", "_").toUpperCase()];
  const required = (key: string) => {
    const value = get(key);
    if (!value) throw new Error(`Missing --${key}`);
    return value;
  };
  const watchMode = get("watch-mode") ?? "changeStream";
  if (watchMode !== "changeStream" && watchMode !== "oplog") throw new Error("Invalid --watch-mode");
  const adopt = get("adopt-legacy-checkpoint") ?? "false";
  if (adopt !== "true" && adopt !== "false") throw new Error("Invalid --adopt-legacy-checkpoint");
  return { mongodbUri: required("mongodb-uri"), database: required("database"),
    collection: required("collection"), rabbitmqUri: required("rabbitmq-uri"), exchange: required("exchange"),
    resumeCollection: get("resume-collection") ?? "tw_dispatcher_state", tenant: get("tenant"),
    watchMode, checkpointKey: get("checkpoint-key"), feedId: get("feed-id"), adoptLegacyCheckpoint: adopt === "true" };
}
