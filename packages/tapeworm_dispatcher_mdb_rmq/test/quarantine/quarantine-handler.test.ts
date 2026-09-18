import { afterEach, expect, test, vi } from "vitest";
import { DeliveryHalted } from "../../src/delivery/delivery-halted";
import { deliverOutcome } from "../../src/delivery/delivery";
import { QuarantineFailureHandler } from "../../src/quarantine/failure-handler";
import { QuarantinePaused } from "../../src/quarantine/errors";
import { MongoQuarantineSourceReader } from "../../src/quarantine/source-reader";
import { encodePublication } from "../../src/rabbitmq/encoding";
import * as references from "../../src/quarantine/validation";
import { MemoryQuarantine, PolicyPublisher, rejectPolicy } from "./support/fixtures";
import { base, handler, handlerScope as scope } from "./support/handler-fixture";
import { commit, MemoryStore, state } from "../support/fixtures";

afterEach(() => { vi.restoreAllMocks(); });
function sizeError(): unknown {
  try { encodePublication(commit(1), rejectPolicy); } catch (error: unknown) { return error; }
  throw new Error("Expected size rejection");
}
function setup(mode?: "pause" | "continue") {
  const store = new MemoryQuarantine(scope);
  const config = { enabled: true as const, store, sourceRetention: "immutable-until-resolved" as const, mode };
  const adapter = handler(config);
  const ready = vi.spyOn(MongoQuarantineSourceReader.prototype, "initialize").mockResolvedValue();
  const initialize = vi.spyOn(store, "initialize"); const reference = vi.spyOn(references, "sourceReference");
  const checkpoint = new MemoryStore();
  const options = { publisher: new PolicyPublisher(rejectPolicy), store: checkpoint, feed: scope.feed,
    collection: "commits", failureHandler: adapter };
  return { store, config, adapter, ready, initialize, reference, checkpoint, options };
}

test.each(["feed", "collection"] as const)("eligible mismatched %s rejects before readiness/hash/storage", async (key) => {
  const f = setup();
  await expect(f.adapter.handle(sizeError(), { commit: commit(1), feed: scope.feed, collection: "commits", [key]: "other" }))
    .rejects.toThrow("scope mismatch");
  expect(f.ready).not.toHaveBeenCalled(); expect(f.initialize).not.toHaveBeenCalled(); expect(f.reference).not.toHaveBeenCalled();
  expect(f.store.writes).toBe(0);
});

test("disabled or ineligible failure declines without checking even mismatched context or touching metadata", async () => {
  const f = setup(); const context = { commit: commit(1), feed: "wrong", collection: "wrong" };
  expect(await f.adapter.handle(new Error("Rabbit unavailable"), context)).toEqual({ kind: "unhandled" });
  expect(await handler({ enabled: false }).handle(sizeError(), context)).toEqual({ kind: "unhandled" });
  expect(f.ready).not.toHaveBeenCalled(); expect(f.initialize).not.toHaveBeenCalled(); expect(f.reference).not.toHaveBeenCalled();
});

test("constructor validates bound store scope cheaply and snapshots feed/config before caller mutation", async () => {
  const f = setup();
  const options = { ...base, mongodb: { ...base.mongodb }, quarantine: f.config };
  const adapter = new QuarantineFailureHandler(options);
  options.mongodb.collection = "other"; f.config.mode = "pause";
  expect(f.ready).not.toHaveBeenCalled(); expect(f.initialize).not.toHaveBeenCalled();
  expect(await adapter.handle(sizeError(), { commit: commit(1), feed: scope.feed, collection: "commits" }))
    .toMatchObject({ kind: "durablyHandled" });
  expect(() => handler({ ...f.config, store: new MemoryQuarantine({ ...scope, feed: "other" }) })).toThrow("scope mismatch");
});

test("failed store readiness resets and retries source then store; success is cached", async () => {
  const f = setup(); const order: string[] = [];
  f.ready.mockImplementation(() => { order.push("source"); return Promise.resolve(); });
  f.initialize.mockImplementationOnce(() => { order.push("store-failed"); return Promise.reject(new Error("Store unavailable")); })
    .mockImplementation(() => { order.push("store"); return Promise.resolve(); });
  const context = { commit: commit(1), feed: scope.feed, collection: "commits" };
  await expect(f.adapter.handle(sizeError(), context)).rejects.toThrow("Store unavailable");
  expect(f.reference).not.toHaveBeenCalled(); expect(f.store.writes).toBe(0);
  await f.adapter.handle(sizeError(), context); await f.adapter.handle(sizeError(), context);
  expect(order).toEqual(["source", "store-failed", "source", "store"]); expect(f.store.writes).toBe(2);
});

test.each(["pause", "continue"] as const)("throwing %s observer is terminal, preserving checkpoint boundary", async (mode) => {
  const f = setup(mode); const cause = new Error("Observer failed");
  f.adapter.on("quarantined", () => { throw cause; });
  const running = deliverOutcome(commit(1), { kind: "replay", state: state() }, f.options);
  await expect(running).rejects.toBeInstanceOf(DeliveryHalted);
  await expect(running).rejects.toHaveProperty("cause", cause);
  expect(f.store.writes).toBe(1); expect(f.checkpoint.saved).toHaveLength(mode === "pause" ? 0 : 1);
});

test("pause retains meaningful error/event and inherits only the generic terminal contract", async () => {
  const f = setup("pause"); const events: unknown[] = [];
  f.adapter.on("quarantined", (event) => { events.push(event); });
  const running = f.adapter.handle(sizeError(), { commit: commit(1), feed: scope.feed, collection: "commits" });
  await expect(running).rejects.toBeInstanceOf(QuarantinePaused);
  await expect(running).rejects.toBeInstanceOf(DeliveryHalted);
  await expect(running).rejects.toMatchObject({ event: events[0] });
  expect(events).toMatchObject([{ checkpointAdvanced: false }]);
});
