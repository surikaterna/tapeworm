import { MongoClient } from "mongodb";
import type { Document } from "mongodb";
import { Dispatcher, ChangeStreamWatcher, OplogWatcher, MongoResumeTokenStore, checkpointFeed,
  MongoQuarantineStore, MongoQuarantineSourceReader, QuarantineService, CommitPublisher, QuarantineFailureHandler } from "tapeworm_dispatcher_mdb_rmq";
import type { DeliveryFailureHandler, DeliveryFailureResult, QuarantineHandlerEvents } from "tapeworm_dispatcher_mdb_rmq";
import type { CommitHandler, IResumeTokenStore, ResumeState, ProgressHandler } from "tapeworm_dispatcher_mdb_rmq";
import type { ICommit } from "tapeworm";
// @ts-expect-error business schema decisions are not a transport API
export type { PublicationDecision } from "tapeworm_dispatcher_mdb_rmq";
import type { RejectionCode } from "tapeworm_dispatcher_mdb_rmq";

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

const quarantine = new MongoQuarantineStore(db, "quarantine", { feed: checkpointFeed(config), sourceCollection: "commits" });
const enabled = { enabled: true as const, store: quarantine, sourceRetention: "immutable-until-resolved" as const };
const adapter = new QuarantineFailureHandler({ ...config, quarantine: enabled });
new Dispatcher({ ...config, resumeTokenStore: scoped, failureHandler: adapter, publication: { maxMessageBytes: 1000 } });
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "continue" } });
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "pause" } });
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "continue", acceptOrderingGaps: true } });
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, acceptOrderingGaps: true } });
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "pause", acceptOrderingGaps: true } });
new QuarantineFailureHandler({ ...config, quarantine: { enabled: false, acceptOrderingGaps: true } });
new QuarantineFailureHandler({ ...config, quarantine: { enabled: false } });
// @ts-expect-error deprecated compatibility field cannot disable ordering gaps
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "continue", acceptOrderingGaps: false } });
// @ts-expect-error false is invalid even with mode omitted
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, acceptOrderingGaps: false } });
// @ts-expect-error false remains invalid on disabled configurations
new QuarantineFailureHandler({ ...config, quarantine: { enabled: false, acceptOrderingGaps: false } });
// @ts-expect-error unsupported quarantine modes are not accepted
new QuarantineFailureHandler({ ...config, quarantine: { ...enabled, mode: "skip" } });
// @ts-expect-error enabled quarantine requires a durable store
new QuarantineFailureHandler({ ...config, quarantine: { enabled: true, sourceRetention: "immutable-until-resolved" } });
// @ts-expect-error enabling quarantine requires an explicit source-retention assertion
new QuarantineFailureHandler({ ...config, quarantine: { enabled: true, store: quarantine } });
// @ts-expect-error business validators are removed, including former valid decisions
new CommitPublisher(config.rabbitmq, undefined, { validateRecord: () => ({ kind: "allow" }) });
// @ts-expect-error no business validator option is accepted
new CommitPublisher(config.rabbitmq, undefined, { validateRecord: () => true });
// @ts-expect-error schema rejection is not a transport rejection
const invalidCode: RejectionCode = "unsupported-schema";
console.log(invalidCode);
adapter.on("quarantined", (event) => {
  const typed: QuarantineHandlerEvents["quarantined"][0] = event;
  const advanced: boolean = event.checkpointAdvanced;
  const resolved: "unresolved" | "published" = event.resolution;
  // @ts-expect-error quarantine operational events do not expose original payloads
  const payload: unknown = event.commit;
  console.log(advanced, resolved, payload, typed);
});
// @ts-expect-error events belong to the adapter, not the dispatcher
dispatcher.on("quarantined", () => {});
// @ts-expect-error obsolete draft dispatcher configuration
new Dispatcher({ ...config, resumeTokenStore: scoped, quarantine: enabled });
// @ts-expect-error obsolete field is rejected even if undefined
new Dispatcher({ ...config, resumeTokenStore: scoped, quarantine: undefined });
const receipt: DeliveryFailureResult = { kind: "durablyHandled", onCheckpointed: () => { console.log("saved"); } };
const failureHandler: DeliveryFailureHandler = { handle: (_error, context) => {
  console.log(context.commit.id, context.feed, context.collection);
  // @ts-expect-error context identity is read-only
  context.commit.id = "other";
  // @ts-expect-error context binding cannot be replaced
  context.feed = "other";
  // @ts-expect-error no checkpoint store is exposed to the handler
  const checkpoint: unknown = context.store;
  console.log(checkpoint); return Promise.resolve(receipt);
} };
new Dispatcher({ ...config, resumeTokenStore: scoped, failureHandler });
// @ts-expect-error only protocol receipt kinds accepted
const wrongKind: DeliveryFailureResult = { kind: "published" };
// @ts-expect-error notification must be synchronous
const asyncReceipt: DeliveryFailureResult = { kind: "durablyHandled", onCheckpointed: async () => {} };
// @ts-expect-error EventEmitter boolean result must not escape the notification
const booleanReceipt: DeliveryFailureResult = { kind: "durablyHandled", onCheckpointed: () => true };
// @ts-expect-error handler must implement the port
new Dispatcher({ ...config, resumeTokenStore: scoped, failureHandler: {} });
// @ts-expect-error null is not an omitted handler
new Dispatcher({ ...config, resumeTokenStore: scoped, failureHandler: null });
console.log(wrongKind, asyncReceipt, booleanReceipt);
const publisher = new CommitPublisher(config.rabbitmq);
declare const validated: ICommit;
declare const untrusted: unknown;
void publisher.publish(validated, "commits");
// @ts-expect-error unknown source data must be validated upstream
void publisher.publish(untrusted, "commits");
const service = new QuarantineService({ ...config, store: quarantine, publisher, sourceRetention: "immutable-until-resolved",
  source: new MongoQuarantineSourceReader(db, "commits", checkpointFeed(config)) });
void service.list({ limit: 25, status: "quarantined" });
void service.redrive("000000000000000000000001", { actor: "operator", reason: "transport limit raised" });
// @ts-expect-error each operator attempt requires actor and reason
void service.redrive("000000000000000000000001", { actor: "operator" });
void service.close(); void publisher.close();
