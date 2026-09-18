import { expect, test } from "vitest";
import { ObjectId, BSON, Long } from "mongodb";
import { commit } from "../support/fixtures";
import { encodePublication, rejectionCode, validatePublicationPolicy, type PublicationPolicy } from "../../src/rabbitmq/encoding";
import { sourceReference, validateQuarantine, validateRejection, validateCompletion } from "../../src/quarantine/validation";
import { MemoryQuarantine, rejectPolicy, scope } from "../quarantine/support/fixtures";
import { CommitPublisher } from "../../src/rabbitmq/publisher";

function eligible(policy: PublicationPolicy): string | undefined {
  try { encodePublication(commit(1), policy); } catch (error: unknown) { return rejectionCode(error); }
  return undefined;
}
test("only configured local byte limit is eligible", () => {
  expect(eligible(rejectPolicy)).toBe("message-too-large");
  expect(eligible({ maxMessageBytes: 1 })).toBe("message-too-large");
  for (const failure of [new Error("nack"), new Error("timeout"), new Error("auth"), new TypeError("encoding")]) {
    expect(rejectionCode(failure)).toBeUndefined();
  }
});
test("UTF8 byte length is checked after actual JSON encoding, inclusive at bound", () => {
  const value = { ...commit(1), domain: "🐛".repeat(10) };
  const body = Buffer.from(JSON.stringify(value));
  expect(body.length).toBeGreaterThan(JSON.stringify(value).length);
  expect(encodePublication(value, { maxMessageBytes: body.length })).toEqual(body);
  expect(() => encodePublication(value, { maxMessageBytes: body.length - 1 })).toThrow("message-too-large");
});
test("serializer exceptions cannot become poison", () => {
  const value = { ...commit(1), toJSON: () => { throw new TypeError("resource bug"); } };
  try { encodePublication(value, { maxMessageBytes: 1 }); } catch (error: unknown) { expect(rejectionCode(error)).toBeUndefined(); }
  expect(() => encodePublication(value, rejectPolicy)).toThrow("serialization failed");
});
test("constructor rejects removed or unsupported JS options even nonenumerable and undefined", () => {
  for (const key of ["validateRecord", "other", Symbol("option")]) {
    const policy: PublicationPolicy = {};
    Object.defineProperty(policy, key, { value: undefined });
    expect(() => new CommitPublisher({ uri: "amqp://localhost", exchange: "e" }, undefined, policy)).toThrow("Unsupported publication option");
  }
  for (const value of [null, [], "old", 1, true, new Date(), { validateRecord: () => ({ kind: "allow" }) }, { maxMessageBytes: "1" }]) {
    expect(() => { validatePublicationPolicy(value); }).toThrow();
    expect(() => { Reflect.construct(CommitPublisher, [{ uri: "amqp://localhost", exchange: "e" }, undefined, value]); }).toThrow();
  }
  const inherited: PublicationPolicy = {};
  Object.setPrototypeOf(inherited, { validateRecord: () => ({ kind: "allow" }) });
  expect(() => new CommitPublisher({ uri: "amqp://localhost", exchange: "e" }, undefined, inherited)).toThrow("plain options");
});
test("removed schema code is rejected by receipt and diagnostic decoders", () => {
  expect(() => { validateRejection("unsupported-schema"); }).toThrow("rejection code");
  expect(() => { validateCompletion("rejected", "unsupported-schema"); }).toThrow("diagnostic");
  for (const diagnostic of ["source-invalid", "publication-failed", "lease-expired", "message-too-large"]) {
    expect(() => { validateCompletion("rejected", diagnostic); }).not.toThrow();
  }
});
test("serialization cannot launder a captured eligible rejection", () => {
  let captured: unknown;
  try { encodePublication(commit(1), rejectPolicy); } catch (error: unknown) { captured = error; }
  const source = { ...commit(1), toJSON: () => { throw captured; } };
  try { encodePublication(source); } catch (error: unknown) { expect(rejectionCode(error)).toBeUndefined(); }
});
test("runtime configuration rejects unsafe limits and invalid enabled quarantine prerequisites", () => {
  for (const maxMessageBytes of [0, -1, 0.5, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => { validatePublicationPolicy({ maxMessageBytes }); }).toThrow("positive integer");
  }
  const store = new MemoryQuarantine(scope);
  const enabled = { enabled: true, store, sourceRetention: "immutable-until-resolved" };
  expect(() => { validateQuarantine({ enabled: "false" }, scope); }).toThrow();
  expect(() => { validateQuarantine({ ...enabled, sourceRetention: "temporary" }, scope); }).toThrow();
  expect(() => { validateQuarantine({ enabled: true, store }, scope); }).toThrow();
  expect(() => { validateQuarantine({ enabled: true, sourceRetention: "immutable-until-resolved" }, scope); }).toThrow();
  expect(() => { validateQuarantine({ ...enabled, mode: "other" }, scope); }).toThrow();
  expect(() => { validateQuarantine({ enabled: false, mode: "other" }, scope); }).toThrow();
});
test("enabled default/explicit continue needs no acknowledgement; legacy true remains compatible", () => {
  const enabled = { enabled: true, store: new MemoryQuarantine(scope), sourceRetention: "immutable-until-resolved" };
  for (const config of [enabled, { ...enabled, mode: "continue" }, { ...enabled, mode: "pause" }, { enabled: false }]) {
    expect(() => { validateQuarantine(config, scope); }).not.toThrow();
    expect(() => { validateQuarantine({ ...config, acceptOrderingGaps: true }, scope); }).not.toThrow();
  }
  expect(() => { validateQuarantine(undefined, scope); }).not.toThrow();
});
test("supplied nontrue legacy acknowledgements reject even when disabled, with explicit-pause guidance", () => {
  const enabled = { enabled: true, store: new MemoryQuarantine(scope), sourceRetention: "immutable-until-resolved" };
  for (const config of [enabled, { ...enabled, mode: "continue" }, { ...enabled, mode: "pause" }, { enabled: false }]) {
    for (const acceptOrderingGaps of [false, null, 0, "true", {}, undefined]) {
      expect(() => { validateQuarantine({ ...config, acceptOrderingGaps }, scope); }).toThrow('mode: "pause"');
    }
  }
});
test("BSON fingerprint preserves domain types and ignores only Mongo top-level _id/order", () => {
  const value = { ...commit(1), nested: { z: new Long(3), a: new Date(0) } };
  const reordered = { _id: new ObjectId(), nested: { a: new Date(0), z: new Long(3) }, ...commit(1) };
  expect(sourceReference(value, scope)).toEqual(sourceReference(reordered, scope));
  expect(sourceReference({ ...value, nested: { z: 3, a: new Date(0) } }, scope).fingerprint)
    .not.toBe(sourceReference(value, scope).fingerprint);
  expect(BSON.serialize(value).length).toBeGreaterThan(0);
});
