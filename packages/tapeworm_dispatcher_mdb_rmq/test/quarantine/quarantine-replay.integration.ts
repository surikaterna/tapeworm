import { afterEach, expect, test, vi } from "vitest";
import { CommitPublisher, MongoResumeTokenStore, MongoQuarantineSourceReader, QuarantineFailureHandler, type QuarantinedEvent } from "../../index";
import { deliverOutcome } from "../../src/delivery/delivery";
import { quarantineFixture } from "./support/integration-fixture";
import { mixedPolicy, PolicyPublisher, request } from "./support/fixtures";
import { state } from "../support/fixtures";
import { rabbit } from "../support/services";
import { record } from "../../src/validation";

afterEach(() => { vi.restoreAllMocks(); });
function forbidden(): never { throw new Error("Healthy quarantine access"); }

test.each(["quarantined", "claimed", "published"] as const)("currently healthy replay delivers original body/id without changing real %s receipt", async (status) => {
  const mq = await rabbit(); const f = await quarantineFixture(60000, mq.exchange);
  const publisher = new CommitPublisher(f.config.rabbitmq);
  try {
    if (status !== "quarantined") {
      const claim = await f.store.claim(f.captured.id, request);
      if (claim.kind !== "claimed") throw new Error("Missing claim");
      if (status === "published") await f.store.finish(f.captured.id, claim.token, "published");
    }
    const before: unknown = await f.mongodb.db.collection("quarantine").findOne({});
    const spies = [vi.spyOn(f.store, "initialize"), vi.spyOn(f.store, "find"), vi.spyOn(f.store, "capture"),
      vi.spyOn(f.store, "list"), vi.spyOn(f.store, "claim"), vi.spyOn(f.store, "finish")];
    for (const spy of spies) spy.mockImplementation(forbidden);
    const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
    const adapter = new QuarantineFailureHandler({ ...f.config,
      quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved", mode: "pause" } });
    const handle = vi.spyOn(adapter, "handle");
    await publisher.connect();
    const result = await deliverOutcome(f.value, { kind: "replay", state: state() }, {
      publisher, store: checkpoints, collection: "commits", feed: f.scope.feed, failureHandler: adapter });
    expect(result.kind).toBe("dispatched"); expect(await checkpoints.load()).not.toBeNull();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    expect(handle).not.toHaveBeenCalled();
    expect(await f.mongodb.db.collection("quarantine").findOne({})).toEqual(before);
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing replay publication");
    expect(record(message.properties).messageId).toBe(f.value.id);
    expect(message.content.toString()).toBe(JSON.stringify(f.value));
  } finally { await publisher.close(); await f.mongodb.close(); await mq.close(); }
});

test("currently rejected published receipt resolves pause; capture retry preserves identity/status/audit after checkpoint failure", async () => {
  const f = await quarantineFixture();
  try {
    const claim = await f.store.claim(f.captured.id, request);
    if (claim.kind !== "claimed") throw new Error("Missing claim");
    await f.store.finish(f.captured.id, claim.token, "published");
    const before = await f.store.find(f.reference);
    const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
    const save = vi.spyOn(checkpoints, "save").mockRejectedValueOnce(new Error("Checkpoint unavailable"));
    const publisher = new PolicyPublisher(mixedPolicy);
    const ready = vi.spyOn(MongoQuarantineSourceReader.prototype, "initialize");
    const adapter = new QuarantineFailureHandler({ ...f.config,
      quarantine: { enabled: true, store: f.store, sourceRetention: "immutable-until-resolved", mode: "pause" } });
    const handle = vi.spyOn(adapter, "handle"); const events: QuarantinedEvent[] = [];
    adapter.on("quarantined", (event) => { events.push(event); });
    const options = { publisher, store: checkpoints, collection: "commits", feed: f.scope.feed, failureHandler: adapter };
    const progress = { kind: "replay" as const, state: state() };
    await expect(deliverOutcome(f.value, progress, options)).rejects.toThrow("Checkpoint unavailable");
    expect(await checkpoints.load()).toBeNull(); expect(events).toEqual([]);
    expect(handle).toHaveBeenCalledTimes(1);
    expect(await deliverOutcome(f.value, progress, options)).toMatchObject({ kind: "handled" });
    expect(events).toMatchObject([{ resolution: "published", checkpointAdvanced: true }]);
    const after = await f.store.find(f.reference);
    expect(after).toMatchObject({ id: before?.id, status: "published", attempts: before?.attempts,
      reference: before?.reference, observations: (before?.observations ?? 0) + 2 });
    expect(save).toHaveBeenCalledTimes(2); expect(ready).toHaveBeenCalledTimes(1); expect(handle).toHaveBeenCalledTimes(2);
    expect(publisher.published).toEqual([]);
  } finally { await f.mongodb.close(); }
});
