import { MongoClient } from "mongodb";
import type { Document } from "mongodb";
import { Dispatcher, ChangeStreamWatcher, OplogWatcher, MongoResumeTokenStore, checkpointFeed } from "tapeworm_dispatcher_mdb_rmq";
import type { CommitHandler, IResumeTokenStore, ResumeState, ProgressHandler } from "tapeworm_dispatcher_mdb_rmq";

const db = new MongoClient("mongodb://localhost").db("consumer");
const legacy = new MongoResumeTokenStore(db, "state");
const config = { mongodb: { db, collection: "commits" }, rabbitmq: { uri: "amqp://localhost", exchange: "commits" }, feedId: "cluster" };
const scoped = new MongoResumeTokenStore(db, "state", { checkpointKey: "scope", feedId: checkpointFeed(config) });
const store: IResumeTokenStore = { load: () => Promise.resolve(null), save: (state: ResumeState) => { console.log(state); return Promise.resolve(); } };
const dispatcher = new Dispatcher({ ...config, resumeTokenStore: scoped });
dispatcher.on("dispatched", (commit) => {
  const id: string = commit.id;
  const payload: unknown = commit.events[0]?.payload;
  console.log(id, payload);
  // @ts-expect-error event payload needs application schema validation
  const invalid: number = commit.events[0]?.payload;
  console.log(invalid);
});
dispatcher.on("recovery", (event) => { const phase: string = event.phase; console.log(phase); });
const compatible: CommitHandler = (commit, token: Document) => { console.log(commit, token); return Promise.resolve(); };
const durable: ProgressHandler = (_commit, progress) => {
  if (progress.kind === "live") { const kind: string = progress.position.kind; console.log(kind); }
  // @ts-expect-error replay/transition progress has no primary position property
  const invalid: unknown = progress.position;
  console.log(invalid);
  return Promise.resolve();
};
const watcher = new ChangeStreamWatcher(config.mongodb);
const oplog = new OplogWatcher(config.mongodb);
void watcher.start(null, compatible); void oplog.start(null, compatible);
void watcher.startWithProgress(null, durable);
console.log(legacy, store);
// @ts-expect-error only supported watch modes are allowed
new Dispatcher({ ...config, resumeTokenStore: legacy, watchMode: "poll" });
