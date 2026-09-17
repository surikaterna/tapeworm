import { expect, test, vi } from "vitest";
import { deliver, deliverOutcome, type DeliveryOptions, type PublisherPort } from "../src/delivery";
import type { DeliveryFailureHandler, DeliveryFailureResult } from "../src/delivery-failure";
import { DeliveryHalted } from "../src/delivery-halted";
import { RecoveryWatcher } from "../src/recovery-watcher";
import { commit, FakeHistory, FakeLive, item, MemoryStore, state, token } from "./fixtures";

const progress = { kind: "replay" as const, state: state() };
function fixture(receipt: DeliveryFailureResult = { kind: "unhandled" }) {
  const error = new Error("Publication failed");
  const publisher = { connect: () => Promise.resolve(), close: () => Promise.resolve(),
    publish: vi.fn((): Promise<void> => Promise.reject(error)) } satisfies PublisherPort;
  const handler = { handle: vi.fn(() => Promise.resolve(receipt)) } satisfies DeliveryFailureHandler;
  const store = new MemoryStore();
  const options: DeliveryOptions = { publisher, store, collection: "commits", feed: "generic-feed", failureHandler: handler };
  return { publisher, handler, store, options, error };
}

test("absent and declining handler preserve original publisher error identity", async () => {
  const f = fixture(); const value = commit(11);
  await expect(deliverOutcome(value, progress, f.options)).rejects.toBe(f.error);
  expect(f.handler.handle).toHaveBeenCalledExactlyOnceWith(f.error, { commit: value, feed: "generic-feed", collection: "commits" });
  f.options.failureHandler = undefined;
  await expect(deliverOutcome(value, progress, f.options)).rejects.toBe(f.error);
  expect(f.store.saved).toEqual([]);
});

test("confirmed success and transitions never invoke handler; legacy deliver reuses checkpoint validation", async () => {
  const f = fixture(); vi.mocked(f.publisher.publish).mockResolvedValue();
  expect((await deliverOutcome(commit(11), progress, f.options)).kind).toBe("dispatched");
  const load = vi.spyOn(f.store, "load");
  expect((await deliverOutcome(undefined, { ...progress, kind: "transition" }, f.options)).kind).toBe("transition");
  expect(load).toHaveBeenCalledTimes(1); expect(f.handler.handle).not.toHaveBeenCalled();
  expect(await deliver(commit(11), progress, f.publisher, f.store, "commits", "generic-feed")).toEqual(f.store.saved.at(-1));
  load.mockResolvedValue(null);
  await expect(deliver(undefined, { ...progress, kind: "transition" }, f.publisher, f.store, "commits", "generic-feed"))
    .rejects.toThrow();
});

test("generic durable handling is awaited before checkpoint and synchronous receipt notification", async () => {
  const order: string[] = []; const notify = vi.fn((): undefined => { order.push("notified"); });
  const f = fixture({ kind: "durablyHandled", onCheckpointed: notify });
  let release: (receipt: DeliveryFailureResult) => void = () => {};
  vi.mocked(f.handler.handle).mockImplementation(() => new Promise((resolve) => { release = resolve; }));
  const save = f.store.save.bind(f.store);
  vi.spyOn(f.store, "save").mockImplementation(async (next) => { await save(next); order.push("saved"); });
  const running = deliverOutcome(commit(11), progress, f.options);
  await vi.waitFor(() => { expect(f.handler.handle).toHaveBeenCalledTimes(1); });
  expect(f.store.saved).toEqual([]); expect(notify).not.toHaveBeenCalled();
  release({ kind: "durablyHandled", onCheckpointed: notify });
  expect((await running).kind).toBe("handled"); expect(order).toEqual(["saved", "notified"]);
});

test.each([null, undefined, false, {}, { kind: "published" }, { kind: "durablyHandled", onCheckpointed: true },
  { kind: "unhandled", onCheckpointed: () => undefined }])("invalid untyped receipt %j fails before checkpoint", async (value: unknown) => {
  const f = fixture();
  Object.defineProperty(f.handler, "handle", { value: () => Promise.resolve(value) });
  await expect(deliverOutcome(commit(11), progress, f.options)).rejects.toThrow("Invalid delivery failure");
  expect(f.store.saved).toEqual([]);
});

test("checkpoint failure does not reenter handler or notify after durable handling or confirmed publication", async () => {
  const notify = vi.fn(() => undefined); const f = fixture({ kind: "durablyHandled", onCheckpointed: notify });
  f.store.failure = true;
  await expect(deliverOutcome(commit(11), progress, f.options)).rejects.toThrow("Save failed");
  expect(f.handler.handle).toHaveBeenCalledTimes(1); expect(notify).not.toHaveBeenCalled();
  vi.mocked(f.publisher.publish).mockResolvedValue();
  await expect(deliverOutcome(commit(11), progress, f.options)).rejects.toThrow("Save failed");
  expect(f.handler.handle).toHaveBeenCalledTimes(1);
});

test("notification waits for the checkpoint save promise, not just its invocation", async () => {
  const notify = vi.fn(() => undefined); const f = fixture({ kind: "durablyHandled", onCheckpointed: notify });
  let release: () => void = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const save = f.store.save.bind(f.store);
  const saving = vi.spyOn(f.store, "save").mockImplementation(async (next) => { await gate; await save(next); });
  const running = deliverOutcome(commit(11), progress, f.options);
  await vi.waitFor(() => { expect(saving).toHaveBeenCalledTimes(1); });
  expect(notify).not.toHaveBeenCalled(); expect(f.store.saved).toEqual([]);
  release(); expect((await running).kind).toBe("handled"); expect(notify).toHaveBeenCalledTimes(1);
});

test("invalid receipt follows normal watcher retry budget without advancing", async () => {
  const f = fixture(); Object.defineProperty(f.handler, "handle", { value: () => Promise.resolve({ kind: "invalid" }) });
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(11); });
  const watcher = new RecoveryWatcher(live, new FakeHistory(), { maxRetries: 3, retryDelayMs: 0 });
  const errors: Error[] = []; watcher.on("error", (error) => { errors.push(error); }); await watcher.connect();
  await expect(watcher.startWithProgress(state(), async (value, next) => { await deliverOutcome(value, next, f.options); }))
    .rejects.toThrow("Invalid delivery failure");
  expect(errors).toHaveLength(2); expect(live.positions).toHaveLength(3); expect(f.store.saved).toEqual([]);
});

const notificationError = new Error("Observer failed");
const notifications = [
  () => { throw notificationError; },
  () => Promise.reject(notificationError),
  () => Promise.resolve(undefined),
  () => new Promise<undefined>(() => {}),
  () => ({ then: (_resolve: unknown, reject: (error: Error) => void) => { reject(notificationError); } }),
  () => true,
];
test.each(notifications)("invalid notification terminally halts without retry; restart loads advanced checkpoint: %s", async (notify) => {
  const receipt: DeliveryFailureResult = { kind: "durablyHandled" };
  Object.defineProperty(receipt, "onCheckpointed", { value: notify });
  const f = fixture(receipt); const unhandled: unknown[] = [];
  const observe = (error: unknown) => { unhandled.push(error); };
  process.on("unhandledRejection", observe);
  try {
    const live = new FakeLive(async function* () { await Promise.resolve(); yield item(11); yield item(12); });
    const watcher = new RecoveryWatcher(live, new FakeHistory(), { maxRetries: 50, retryDelayMs: 0 });
    const retried = vi.fn(); watcher.on("error", retried); await watcher.connect();
    const running = watcher.startWithProgress(state(), async (value, next) => { await deliverOutcome(value, next, f.options); });
    await expect(running).rejects.toBeInstanceOf(DeliveryHalted);
    await expect(running).rejects.toHaveProperty("cause");
    if (notify === notifications[0]) await expect(running).rejects.toHaveProperty("cause", notificationError);
    expect(f.handler.handle).toHaveBeenCalledTimes(1); expect(live.positions).toHaveLength(1); expect(retried).not.toHaveBeenCalled();
    expect((await f.store.load())?.lastCommitToken).toBe(token(11));
    await restart(f.options, f.store);
    await new Promise((resolve) => { setImmediate(resolve); }); expect(unhandled).toEqual([]);
  } finally { process.removeListener("unhandledRejection", observe); }
});

async function restart(options: DeliveryOptions, store: MemoryStore): Promise<void> {
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(12); });
  const watcher = new RecoveryWatcher(live, new FakeHistory(), { maxRetries: 50, retryDelayMs: 0 });
  await watcher.connect(); vi.spyOn(options.publisher, "publish").mockResolvedValue();
  await watcher.startWithProgress(await store.load(), async (value, next) => {
    await deliverOutcome(value, next, options); await watcher.stop();
  });
  expect(live.positions).toEqual([{ kind: "changeStream", token: { _data: "11" } }]);
  expect((await store.load())?.lastCommitToken).toBe(token(12));
}

test("receipt accessors are read once before save; watcher notifies without stale retry", async () => {
  let saved = false; let kindReads = 0; let callbackReads = 0; let notified = 0;
  const original = new Error("Handler receipt read after checkpoint");
  const receipt: DeliveryFailureResult = {
    get kind(): "durablyHandled" { kindReads++; return "durablyHandled"; },
    get onCheckpointed(): () => undefined {
      callbackReads++;
      if (saved) throw original;
      return () => { expect(saved).toBe(true); notified++; };
    },
  };
  const f = fixture(receipt); const save = f.store.save.bind(f.store);
  vi.spyOn(f.store, "save").mockImplementation(async (next) => { await save(next); saved = true; });
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(11); });
  const watcher = new RecoveryWatcher(live, new FakeHistory(), { maxRetries: 2, retryDelayMs: 0 });
  const retry = vi.fn(); watcher.on("error", retry); await watcher.connect();
  const outcomes: string[] = [];
  const error = await watcher.startWithProgress(state(), async (value, next) => {
    outcomes.push((await deliverOutcome(value, next, f.options)).kind); await watcher.stop();
  }).catch((cause: unknown) => cause);
  const observed = { saved: f.store.saved.length, publicationAttempts: f.publisher.publish.mock.calls.length,
    retries: retry.mock.calls.length, notified, terminal: error instanceof DeliveryHalted, originalError: error === original,
    kindReads, callbackReads, outcomes };
  console.info("Receipt accessor boundary", observed);
  expect(observed).toEqual({ saved: 1, publicationAttempts: 1, retries: 0, notified: 1, terminal: false,
    originalError: false, kindReads: 1, callbackReads: 1, outcomes: ["handled"] });
  expect(error).toBeUndefined();
});

test("handler receipt mutation during deferred checkpoint cannot fabricate publication or replace notification", async () => {
  const notify = vi.fn(() => undefined); const replacement = vi.fn(() => undefined);
  const receipt: DeliveryFailureResult = { kind: "durablyHandled", onCheckpointed: notify };
  const f = fixture(receipt); const save = f.store.save.bind(f.store);
  let release: () => void = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const saving = vi.spyOn(f.store, "save").mockImplementation(async (next) => {
    expect(Reflect.set(receipt, "kind", "unhandled")).toBe(true);
    expect(Reflect.set(receipt, "onCheckpointed", replacement)).toBe(true);
    await gate; await save(next);
  });
  const running = deliverOutcome(commit(11), progress, f.options);
  await vi.waitFor(() => { expect(saving).toHaveBeenCalledTimes(1); });
  expect(f.store.saved).toEqual([]); expect(notify).not.toHaveBeenCalled();
  release(); const outcome = await running;
  console.info("Receipt mutation boundary", { publicationSucceeded: false, outcome: outcome.kind, saved: f.store.saved.length });
  expect(outcome.kind).toBe("handled"); expect(f.store.saved).toHaveLength(1);
  expect(notify).toHaveBeenCalledTimes(1); expect(replacement).not.toHaveBeenCalled();
});

test.each(["kind", "onCheckpointed"])("throwing receipt %s getter fails before checkpoint", async (key) => {
  const original = new Error("Receipt accessor failed"); const receipt: DeliveryFailureResult = { kind: "durablyHandled" };
  let reads = 0;
  Object.defineProperty(receipt, key, { get: () => { reads++; throw original; } });
  const f = fixture(receipt); const save = vi.spyOn(f.store, "save");
  await expect(deliverOutcome(commit(11), progress, f.options)).rejects.toBe(original);
  expect(save).not.toHaveBeenCalled(); expect(f.store.saved).toEqual([]); expect(reads).toBe(1);
});
