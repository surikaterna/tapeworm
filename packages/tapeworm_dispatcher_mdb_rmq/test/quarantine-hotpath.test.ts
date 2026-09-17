import { EventEmitter } from "node:events";
import * as crypto from "node:crypto";
import { BSON } from "mongodb";
import { afterEach, expect, test, vi } from "vitest";
import * as validation from "../src/validation";
import * as references from "../src/quarantine/validation";
import * as policy from "../src/publication-policy";
import { CommitPublisher } from "../src/publisher";
import { ConfirmedChannel, type ConfirmPort, type PublishProperties } from "../src/confirmed-channel";
import { deliverOutcome, type DeliveryOptions } from "../src/quarantine/delivery";
import { commit, MemoryStore, state } from "./fixtures";
import { MemoryQuarantine, PolicyPublisher, rejectPolicy, scope } from "./quarantine-fixtures";

vi.mock("node:crypto", { spy: true });
vi.mock("mongodb", async (importOriginal) => {
  const actual = await importOriginal<typeof import("mongodb")>();
  return { ...actual, BSON: { ...actual.BSON, serialize: vi.fn(actual.BSON.serialize) } };
});
afterEach(() => { vi.restoreAllMocks(); });

function forbidQuarantine(store: MemoryQuarantine): void {
  const forbidden = () => { throw new Error("Healthy quarantine access"); };
  store.initialize = forbidden; store.find = forbidden; store.capture = forbidden;
  store.list = forbidden; store.claim = forbidden; store.finish = forbidden;
}

test.each([undefined, "quarantined", "claimed", "published"] as const)(
  "healthy replay ignores historical receipt %s without hashing, decoding or store access", async (status) => {
  const store = new MemoryQuarantine(scope);
  if (status) {
    const captured = await store.capture(references.sourceReference(commit(1), scope), "message-too-large");
    store.record = { ...captured, status };
  }
  const before = store.record;
  forbidQuarantine(store);
  vi.clearAllMocks();
  const reference = vi.spyOn(references, "sourceReference");
  const hash = vi.spyOn(crypto, "createHash");
  const bson = vi.spyOn(BSON, "serialize");
  const decode = vi.spyOn(validation, "decodeCommit");
  const publisher = new PolicyPublisher(); const checkpoint = new MemoryStore();
  const prepareQuarantine = vi.fn(() => { throw new Error("Healthy readiness access"); });
  const result = await deliverOutcome(commit(1), { kind: "replay", state: state() }, {
    publisher, store: checkpoint, collection: "commits", feed: "feed", prepareQuarantine,
    quarantine: { enabled: true, store, sourceRetention: "immutable-until-resolved", mode: "pause" } });
  expect(result.kind).toBe("dispatched"); expect(publisher.published).toEqual([commit(1)]);
  expect(checkpoint.saved).toHaveLength(1); expect(store.record).toBe(before);
  for (const spy of [reference, hash, bson, decode, prepareQuarantine]) expect(spy).not.toHaveBeenCalled();
});

class Channel extends EventEmitter implements ConfirmPort {
  attempts: { body: Buffer; options: PublishProperties; confirm: (error: unknown) => void }[] = [];
  publish(_exchange: string, _key: string, body: Buffer, options: PublishProperties,
    confirm: (error: unknown) => void): boolean {
    this.attempts.push({ body, options, confirm }); return true;
  }
}
function publisherFixture(publication?: policy.PublicationPolicy) {
  const channel = new Channel();
  const confirmed = new ConfirmedChannel(channel, 1000, 1, () => {});
  const publisher = new CommitPublisher({ uri: "amqp://localhost", exchange: "e" }, "tenant", publication);
  // Inject the real confirm implementation without a network connection in this unit test.
  Object.defineProperty(publisher, "confirmed", { value: confirmed, writable: true });
  return { channel, confirmed, publisher };
}
test("normal publisher encodes one JSON/UTF8 Buffer, keeps headers/id and checkpoints only after confirm", async () => {
  let serialized = 0;
  const value = commit(1);
  value.events[0] = { id: "event", type: "test", payload: { toJSON: () => { serialized++; return { text: "🐛" }; } } };
  const expected = Buffer.from(JSON.stringify(value)); serialized = 0;
  const { channel, publisher } = publisherFixture({ maxMessageBytes: expected.length });
  const configValidation = vi.spyOn(policy, "validatePublicationPolicy");
  const decode = vi.spyOn(validation, "decodeCommit"); const encode = vi.spyOn(policy, "encodePublication");
  const buffer = vi.spyOn(Buffer, "from");
  const checkpoint = new MemoryStore();
  const delivery = deliverOutcome(value, { kind: "replay", state: state() }, {
    publisher, store: checkpoint, collection: "commits", feed: "feed", prepareQuarantine: () => Promise.resolve() });
  const bufferCalls = buffer.mock.calls.length; buffer.mockRestore();
  expect(bufferCalls).toBe(1); expect(serialized).toBe(1); expect(encode).toHaveBeenCalledTimes(1);
  expect(configValidation).not.toHaveBeenCalled();
  expect(decode).not.toHaveBeenCalled(); expect(checkpoint.saved).toEqual([]);
  const attempt = channel.attempts[0]; if (!attempt) throw new Error("Missing wire attempt");
  expect(attempt.body).toEqual(expected);
  expect(attempt.options).toMatchObject({ messageId: value.id, contentType: "application/json", deliveryMode: 2,
    mandatory: true, headers: { collection: "commits", tenant: "tenant", partitionId: value.partitionId, streamId: value.streamId } });
  attempt.confirm(undefined); await delivery;
  expect(checkpoint.saved).toHaveLength(1); await publisher.close();
});
test.each([new Error("Rabbit unavailable"), new TypeError("Unexpected implementation bug")])(
  "ordinary publication failure %s never enters quarantine", async (failure) => {
  const quarantine = new MemoryQuarantine(scope); forbidQuarantine(quarantine);
  const publisher = new PolicyPublisher(rejectPolicy); publisher.failure = failure;
  const checkpoint = new MemoryStore();
  const prepareQuarantine = vi.fn(() => { throw new Error("Unexpected readiness"); });
  const options: DeliveryOptions = { publisher, store: checkpoint, collection: "commits", feed: "feed", prepareQuarantine,
    quarantine: { enabled: true, store: quarantine, sourceRetention: "immutable-until-resolved" } };
  await expect(deliverOutcome(commit(1), { kind: "replay", state: state() }, options)).rejects.toBe(failure);
  expect(prepareQuarantine).not.toHaveBeenCalled(); expect(checkpoint.saved).toEqual([]);
  expect((await deliverOutcome(undefined, { kind: "transition", state: state() }, options)).kind).toBe("transition");
  expect(prepareQuarantine).not.toHaveBeenCalled(); expect(checkpoint.saved).toHaveLength(1);
});
test("actual publisher size rejection makes zero network calls; capacity fails before encoding", async () => {
  const { channel, confirmed, publisher } = publisherFixture(rejectPolicy);
  await expect(publisher.publish(commit(1), "commits")).rejects.toThrow("message-too-large");
  expect(channel.attempts).toEqual([]);
  const pending = confirmed.publish("e", Buffer.from("pending"), {});
  const encode = vi.spyOn(policy, "encodePublication");
  await expect(publisher.publish(commit(1), "commits")).rejects.toThrow("capacity");
  expect(encode).not.toHaveBeenCalled();
  channel.attempts[0]?.confirm(undefined); await pending; await publisher.close();
});
test("readiness failure never fingerprints, captures or checkpoints; retry and changed fingerprint fail closed", async () => {
  const quarantine = new MemoryQuarantine(scope); const checkpoint = new MemoryStore();
  const prepareQuarantine = vi.fn(() => Promise.reject(new Error("Index unavailable")));
  const options: DeliveryOptions = { publisher: new PolicyPublisher(rejectPolicy), store: checkpoint,
    collection: "commits", feed: "feed", prepareQuarantine,
    quarantine: { enabled: true, store: quarantine, sourceRetention: "immutable-until-resolved" } };
  const reference = vi.spyOn(references, "sourceReference");
  await expect(deliverOutcome(commit(1), { kind: "replay", state: state() }, options)).rejects.toThrow("Index unavailable");
  expect(reference).not.toHaveBeenCalled(); expect(quarantine.writes).toBe(0); expect(checkpoint.saved).toEqual([]);
  options.prepareQuarantine = () => Promise.resolve();
  await deliverOutcome(commit(1), { kind: "replay", state: state() }, options);
  await expect(deliverOutcome({ ...commit(1), domain: "changed" }, { kind: "replay", state: state() }, options)).rejects.toThrow("fingerprint");
  expect(checkpoint.saved).toHaveLength(1);
});
