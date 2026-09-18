import { expect, test } from "vitest";
import { CommitPublisher, MongoResumeTokenStore, QuarantineService } from "../../index";
import { record } from "../../src/validation";
import { quarantineFixture } from "./support/integration-fixture";
import { request } from "./support/fixtures";
import { rabbit } from "../support/services";
import { state } from "../support/fixtures";
import { encodePublication } from "../../src/rabbitmq/encoding";

test.each(["changeStream", "oplog"] as const)("real redrive preserves original body/id and checkpoint in %s feed", async (mode) => {
  const mq = await rabbit();
  const f = await quarantineFixture(60000, mq.exchange, mode);
  const maxMessageBytes = encodePublication(f.value).length;
  const publisher = new CommitPublisher(f.config.rabbitmq, undefined, { maxMessageBytes });
  const service = new QuarantineService({ ...f.config, store: f.store, source: f.source, publisher,
    publication: { maxMessageBytes }, sourceRetention: "immutable-until-resolved" });
  const rejected = new QuarantineService({ ...f.config, store: f.store, source: f.source, publisher,
    publication: { maxMessageBytes: maxMessageBytes - 1 }, sourceRetention: "immutable-until-resolved" });
  try {
    const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
    await checkpoints.save(state()); const before = await checkpoints.load();
    expect(await rejected.redrive(f.captured.id, request)).toEqual({ kind: "rejected", code: "message-too-large" });
    expect(await mq.channel.get(mq.queue, { noAck: true })).toBe(false);
    expect(await service.redrive(f.captured.id, request)).toEqual({ kind: "published" });
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing original publication");
    expect(record(message.properties).messageId).toBe(f.value.id);
    const source = await f.source.read(f.reference);
    expect(message.content.toString()).toBe(JSON.stringify(source));
    expect((await f.store.find(f.reference))?.attempts.map((attempt) => attempt.result)).toEqual(["rejected", "published"]);
    expect(await service.redrive(f.captured.id, request)).toEqual({ kind: "already-published" });
    expect(await mq.channel.get(mq.queue, { noAck: true })).toBe(false);
    expect(await checkpoints.load()).toEqual(before);
  } finally { await rejected.close(); await service.close(); await publisher.close(); await f.mongodb.close(); await mq.close(); }
});
test("real Mongo completion rejection after Rabbit confirm remains unknown and leaves primary checkpoint untouched", async () => {
  const mq = await rabbit();
  const f = await quarantineFixture(60000, mq.exchange);
  const publisher = new CommitPublisher(f.config.rabbitmq);
  const service = new QuarantineService({ ...f.config, store: f.store, source: f.source, publisher,
    sourceRetention: "immutable-until-resolved" });
  try {
    const checkpoints = new MongoResumeTokenStore(f.mongodb.db, "checkpoint");
    await checkpoints.save(state()); const before = await checkpoints.load();
    await f.mongodb.db.command({ collMod: "quarantine", validator: { $expr: { $ne: ["$status", "published"] } } });
    expect(await service.redrive(f.captured.id, request)).toEqual({ kind: "outcome-unknown" });
    const message = await mq.channel.get(mq.queue, { noAck: true });
    if (!message) throw new Error("Missing confirmed publication");
    expect(record(message.properties).messageId).toBe(f.value.id);
    expect((await f.store.find(f.reference))?.status).toBe("claimed");
    expect(await checkpoints.load()).toEqual(before);
  } finally { await service.close(); await publisher.close(); await f.mongodb.close(); await mq.close(); }
});
