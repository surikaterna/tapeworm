import type { MongoConfig } from "../types";
import { MongoHistory } from "./history";
import { OplogSource } from "./live-source";
import { RecoveryWatcher } from "./recovery-watcher";

/** Direct inserts only: not majority safe, no transactional applyOps support. */
export class OplogWatcher extends RecoveryWatcher {
  constructor(config: MongoConfig) { super(new OplogSource(config), new MongoHistory(config), config); }
}
