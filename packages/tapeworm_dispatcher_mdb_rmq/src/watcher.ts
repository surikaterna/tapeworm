import type { MongoConfig } from "./types";
import { MongoHistory } from "./history";
import { ChangeStreamSource } from "./live-source";
import { RecoveryWatcher } from "./recovery-watcher";
export type { CommitHandler, ICommitWatcher, ProgressHandler, DurableCommitWatcher } from "./recovery-watcher";

export class ChangeStreamWatcher extends RecoveryWatcher {
  constructor(config: MongoConfig) { super(new ChangeStreamSource(config), new MongoHistory(config), config); }
}
