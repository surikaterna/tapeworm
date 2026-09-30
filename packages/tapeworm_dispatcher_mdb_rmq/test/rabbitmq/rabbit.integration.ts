import { expect, test } from "vitest";
import { CommitPublisher } from "../../src/rabbitmq/publisher";
import { commit } from "../support/fixtures";
import { rabbit, rabbitUri, eventually } from "../support/services";
import { rabbitProxy } from "../support/rabbit-proxy";

test("real mandatory return wins over ACK, durable routing delivers stable messageId", async () => {
  const env = await rabbit();
  const publisher = new CommitPublisher({ uri: rabbitUri, exchange: env.exchange, confirmTimeoutMs: 3000 });
  try {
    await publisher.connect();
    await expect(publisher.publish(commit(11), "unbound")).rejects.toThrow("unroutable");
    await publisher.publish(commit(11), "commits");
    const message = await env.channel.get(env.queue, { noAck: true });
    if (!message) throw new Error("No routed message");
    expect(message.properties.messageId).toBe("commit-11");
    expect(message.properties.deliveryMode).toBe(2);
  } finally { await publisher.close(); await env.close(); }
});

test("real headers binding routes only the first event's aggregate type", async () => {
  const env = await rabbit();
  const typedQueue = `${env.queue}_typed`;
  const publisher = new CommitPublisher({ uri: rabbitUri, exchange: env.exchange, confirmTimeoutMs: 3000 }, "acme");
  try {
    await env.channel.assertQueue(typedQueue, { durable: true });
    await env.channel.bindQueue(typedQueue, env.exchange, "", {
      "x-match": "all", collection: "commits", aggregateType: "billing",
    });
    await publisher.connect();
    const first = commit(21);
    first.events = [{ id: "first", type: "billing.invoice.paid", payload: {} },
      { id: "other", type: "shipping.sent", payload: {} }];
    await publisher.publish(first, "commits");
    const routed = await env.channel.get(typedQueue, { noAck: true });
    if (!routed) throw new Error("No aggregate-type routed message");
    expect(routed.properties.headers).toMatchObject({ collection: "commits", partitionId: "master",
      streamId: "stream", tenant: "acme", aggregateType: "billing" });
    expect(routed.properties).toMatchObject({ contentType: "application/json", deliveryMode: 2,
      messageId: first.id });
    expect(routed.properties.correlationId).toEqual(expect.any(String));
    expect(routed.content.toString()).toBe(JSON.stringify(first));

    const invalid = commit(22);
    invalid.events = [];
    await publisher.publish(invalid, "commits");
    const fallback = await env.channel.get(env.queue, { noAck: true });
    if (!fallback) throw new Error("No fallback message");
    expect(fallback.properties.messageId).toBe(first.id);
    const omitted = await env.channel.get(env.queue, { noAck: true });
    if (!omitted) throw new Error("No published empty-event message");
    expect(omitted.properties.headers).not.toHaveProperty("aggregateType");
    expect(omitted.properties.messageId).toBe(invalid.id);
    expect(await env.channel.get(typedQueue, { noAck: true })).toBe(false);
  } finally {
    await publisher.close();
    await env.channel.deleteQueue(typedQueue);
    await env.close();
  }
});

test("channel-only broker close reconnects; stop rejects future publication", async () => {
  const env = await rabbit();
  const publisher = new CommitPublisher({ uri: rabbitUri, exchange: env.exchange, confirmTimeoutMs: 3000 });
  try {
    await publisher.connect();
    await env.channel.deleteExchange(env.exchange);
    await expect(publisher.publish(commit(11), "commits")).rejects.toThrow();
    await env.channel.assertExchange(env.exchange, "headers", { durable: true });
    await env.channel.bindQueue(env.queue, env.exchange, "", { "x-match": "all", collection: "commits" });
    await eventually(async () => {
      try { await publisher.publish(commit(12), "commits"); return true; }
      catch { return false; }
    });
    const message = await env.channel.get(env.queue, { noAck: true });
    if (!message) throw new Error("No message after reconnect");
    expect(message.properties.messageId).toBe("commit-12");
  } finally { await publisher.close(); await env.close(); }
  await expect(publisher.publish(commit(12), "commits")).rejects.toThrow("stopped");
});

test("real TCP disconnect shares one reconnect loop and cleans sockets on stop", async () => {
  const env = await rabbit();
  const proxy = await rabbitProxy(rabbitUri);
  const publisher = new CommitPublisher({ uri: proxy.uri, exchange: env.exchange, confirmTimeoutMs: 3000 });
  try {
    await Promise.all([publisher.connect(), publisher.connect()]);
    expect(proxy.connections()).toBe(1);
    proxy.disconnect();
    await eventually(async () => {
      if (proxy.connections() < 2) return false;
      try { await publisher.publish(commit(11), "commits"); return true; } catch { return false; }
    });
    expect(proxy.connections()).toBe(2);
    await publisher.close();
    await eventually(() => Promise.resolve(proxy.sockets() === 0));
    expect(proxy.sockets()).toBe(0);
  } finally { await publisher.close(); await proxy.close(); await env.close(); }
});
