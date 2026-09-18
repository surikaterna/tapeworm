import { MongoClient } from "mongodb";
import { expect, test } from "vitest";
import { QuarantineService } from "../../src/quarantine/service";
import { checkpointFeed } from "../../src/checkpoints/feed";
import { sourceReference } from "../../src/quarantine/validation";
import { commit } from "../support/fixtures";
import { MemoryQuarantine, PolicyPublisher, rejectPolicy, request } from "./support/fixtures";

async function setup() {
  const config = { mongodb: { db: new MongoClient("mongodb://localhost").db("unit"), collection: "commits" },
    rabbitmq: { uri: "amqp://localhost", exchange: "exchange" } };
  const scope = { feed: checkpointFeed(config), sourceCollection: "commits" };
  const store = new MemoryQuarantine(scope);
  const publisher = new PolicyPublisher();
  const source = { scope, initialize: () => Promise.resolve(), read: () => Promise.resolve(commit(1)) };
  const captured = await store.capture(sourceReference(commit(1), scope), "message-too-large");
  const options = { ...config, store, publisher, source, sourceRetention: "immutable-until-resolved" as const };
  return { store, publisher, source, captured, options };
}
test("explicit redrive confirms original identity then completes, already published never republishes", async () => {
  const { options, publisher, captured, store } = await setup();
  const service = new QuarantineService(options);
  expect(await service.list()).toMatchObject({ records: [{ id: captured.id }] });
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "published" });
  expect(publisher.published).toEqual([commit(1)]);
  expect(store.record?.attempts[0]).toMatchObject({ ...request, result: "published" });
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "already-published" });
  await service.close(); expect(publisher.closed).toBe(false);
  await expect(service.redrive(captured.id, request)).rejects.toThrow("closed");
  await publisher.close();
});
test("lost claim acknowledgement is unknown, never publishes or automatically retries", async () => {
  const { options, publisher, captured, store } = await setup();
  const claim = store.claim.bind(store);
  store.claim = async (id, details) => { await claim(id, details); throw new Error("secret claim connection URI"); };
  const service = new QuarantineService(options);
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "outcome-unknown" });
  expect(store.record?.status).toBe("claimed"); expect(store.record?.attempts).toHaveLength(1);
  expect(publisher.calls).toBe(0); await service.close();
});
test("simultaneous operator attempts are busy and close drains without closing caller-owned publisher", async () => {
  const { options, publisher, captured } = await setup();
  let release: () => void = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let began: () => void = () => {};
  const publishing = new Promise<void>((resolve) => { began = resolve; });
  publisher.publish = () => { began(); return gate; };
  const service = new QuarantineService(options);
  const first = service.redrive(captured.id, request);
  await publishing;
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "busy" });
  let closed = false;
  const closing = service.close().then(() => { closed = true; });
  await Promise.resolve(); expect(closed).toBe(false); expect(publisher.closed).toBe(false);
  release(); expect(await first).toEqual({ kind: "published" }); await closing;
  expect(closed).toBe(true); expect(publisher.closed).toBe(false);
});
test("current permanent policy and missing source are audited rejections without network", async () => {
  const { options, publisher, captured, store, source } = await setup();
  const service = new QuarantineService({ ...options, publication: rejectPolicy });
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "rejected", code: "message-too-large" });
  source.read = () => Promise.reject(new Error("credentials must not escape"));
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "rejected", code: "source-invalid" });
  expect(publisher.calls).toBe(0); expect(store.record?.status).toBe("quarantined"); await service.close();
});
test("broker and completion failures remain uncertain without false published state", async () => {
  const { options, publisher, captured, store } = await setup();
  const service = new QuarantineService(options);
  publisher.failure = new Error("secret URI");
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "outcome-unknown" });
  expect(store.record?.status).toBe("quarantined");
  publisher.failure = undefined; store.finishFailure = true;
  expect(await service.redrive(captured.id, request)).toEqual({ kind: "outcome-unknown" });
  expect(store.record?.status).toBe("claimed"); await service.close();
});
test("actor and reason required and bounded by UTF8 bytes", async () => {
  const { options, captured } = await setup();
  const service = new QuarantineService(options);
  expect(() => service.redrive(captured.id, { ...request, actor: "" })).toThrow();
  expect(() => service.redrive(captured.id, { ...request, actor: "🐛".repeat(33) })).toThrow();
  expect(() => service.redrive(captured.id, { ...request, reason: "🐛".repeat(257) })).toThrow();
  await service.close();
});
