import { afterEach, expect, test, vi } from "vitest";
import { deliverOutcome, type DeliveryOptions } from "../../src/delivery/delivery";
import { MongoQuarantineSourceReader } from "../../src/quarantine/source-reader";
import { handler, handlerScope as scope } from "./support/handler-fixture";
import { QuarantinePaused } from "../../src/quarantine/errors";
import { RecoveryWatcher } from "../../src/ingestion/recovery-watcher";
import { commit, FakeHistory, FakeLive, item, MemoryStore, state } from "../support/fixtures";
import { MemoryQuarantine, PolicyPublisher, rejectPolicy, mixedPolicy, oversizedCommit } from "./support/fixtures";
import type { QuarantineConfig, QuarantinedEvent } from "../../src/quarantine/types";

afterEach(() => { vi.restoreAllMocks(); });

function setup(mode?: "pause" | "continue") {
  const quarantine = new MemoryQuarantine(scope);
  const publisher = new PolicyPublisher(rejectPolicy);
  const store = new MemoryStore();
  vi.spyOn(MongoQuarantineSourceReader.prototype, "initialize").mockResolvedValue();
  const config: QuarantineConfig = {
    enabled: true, store: quarantine, sourceRetention: "immutable-until-resolved", ...(mode ? { mode } : {}) };
  const adapter = handler(config);
  const events: QuarantinedEvent[] = [];
  adapter.on("quarantined", (event) => { events.push(event); });
  const options: DeliveryOptions = { publisher, store, collection: "commits", feed: scope.feed, failureHandler: adapter };
  return { quarantine, publisher, store, options, config, adapter, events };
}
const progress = { kind: "replay" as const, state: state() };
test.each([undefined, { enabled: false }] as const)("absent/disabled quarantine %j performs zero lookup and stays fail-closed", async (config) => {
  const { options, quarantine, store } = setup();
  options.failureHandler = config ? handler(config) : undefined;
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toThrow("message-too-large");
  expect(quarantine.lookups).toBe(0); expect(store.saved).toEqual([]);
  options.publisher = new PolicyPublisher();
  expect((await deliverOutcome(commit(1), progress, options)).kind).toBe("dispatched");
});
test("pause re-evaluates and captures idempotently, never checkpoints", async () => {
  const { options, quarantine, publisher, store } = setup("pause");
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toMatchObject({
    event: { checkpointAdvanced: false, resolution: "unresolved" } });
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toBeInstanceOf(QuarantinePaused);
  expect(quarantine.writes).toBe(2); expect(publisher.calls).toBe(2); expect(publisher.published).toEqual([]); expect(store.saved).toEqual([]);
});
test.each([undefined, "continue"] as const)("enabled mode %s continues through poison and healthy records without a flag", async (mode) => {
  const { options, quarantine, store, config, events } = setup(mode);
  const publisher = new PolicyPublisher(mixedPolicy);
  options.publisher = publisher;
  expect(config).not.toHaveProperty("acceptOrderingGaps");
  if (mode === undefined) expect(config).not.toHaveProperty("mode");
  expect((await deliverOutcome(oversizedCommit(1), progress, options))).toMatchObject({ kind: "handled" });
  expect(events).toMatchObject([{ checkpointAdvanced: true }]);
  expect(quarantine.record?.status).toBe("quarantined"); expect(store.saved).toHaveLength(1);
  expect((await deliverOutcome(commit(2), progress, options)).kind).toBe("dispatched");
  expect(publisher.published).toEqual([commit(2)]); expect(quarantine.lookups).toBe(0);
  expect(config.enabled).toBe(true); expect(store.saved).toHaveLength(2);
});
test("default continue waits for durable capture before checkpointing", async () => {
  const { options, quarantine, store, events } = setup();
  const capture = quarantine.capture.bind(quarantine);
  let release: () => void = () => {};
  let began: () => void = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const started = new Promise<void>((resolve) => { began = resolve; });
  const order: string[] = [];
  quarantine.capture = async (reference, code) => {
    began(); await gate;
    const captured = await capture(reference, code); order.push("capture-durable"); return captured;
  };
  const save = store.save.bind(store);
  store.save = async (value) => { order.push("checkpoint"); await save(value); };
  const delivery = deliverOutcome(commit(1), progress, options);
  await started; expect(store.saved).toEqual([]); expect(quarantine.record).toBeUndefined();
  release(); expect(await delivery).toMatchObject({ kind: "handled" });
  expect(events).toMatchObject([{ checkpointAdvanced: true }]);
  expect(order).toEqual(["capture-durable", "checkpoint"]);
});
test("capture failure and infrastructure failure cannot advance position", async () => {
  const { options, quarantine, store, publisher } = setup();
  quarantine.failure = true;
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toThrow("Capture failed");
  expect(store.saved).toEqual([]);
  quarantine.failure = false; publisher.failure = new Error("Rabbit unavailable");
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toThrow("Rabbit unavailable");
  expect(quarantine.writes).toBe(1); expect(store.saved).toEqual([]);
});
test("checkpoint failure keeps capture recoverable; currently rejected published receipt resolves even pause", async () => {
  const { options, quarantine, store, publisher, events } = setup("pause");
  options.failureHandler = handler({ enabled: true, store: quarantine, sourceRetention: "immutable-until-resolved" });
  store.failure = true;
  await expect(deliverOutcome(commit(1), progress, options)).rejects.toThrow("Save failed");
  const captured = quarantine.record;
  store.failure = false;
  expect((await deliverOutcome(commit(1), progress, options)).kind).toBe("handled");
  expect(quarantine.record).toBe(captured);
  if (!quarantine.record) throw new Error("Missing capture");
  quarantine.record = { ...quarantine.record, status: "published" };
  const adapter = handler({ enabled: true, store: quarantine, sourceRetention: "immutable-until-resolved", mode: "pause" });
  adapter.on("quarantined", (event) => { events.push(event); });
  options.failureHandler = adapter;
  expect(await deliverOutcome(commit(1), progress, options)).toMatchObject({ kind: "handled" });
  expect(events).toMatchObject([{ resolution: "published" }]);
  expect(publisher.calls).toBe(3); expect(publisher.published).toEqual([]);
});
test.each(["changeStream", "oplog"] as const)("pause terminally stops common watcher in %s", async (mode) => {
  const { options, publisher, store } = setup("pause");
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(1); yield item(2); });
  const port = { mode, watch: live.watch.bind(live), close: live.close.bind(live) };
  const watcher = new RecoveryWatcher(port, new FakeHistory(), { maxRetries: 50, retryDelayMs: 0 });
  await watcher.connect();
  await expect(watcher.startWithProgress(state(), async (value, next) => { await deliverOutcome(value, next, options); }))
    .rejects.toBeInstanceOf(QuarantinePaused);
  expect(publisher.calls).toBe(1); expect(live.positions).toHaveLength(1); expect(store.saved).toEqual([]);
});
