import { expect, test } from "vitest";
import { CommitPublisher } from "../src/publisher";
import { commit } from "./fixtures";
import { rabbit, rabbitUri, eventually } from "./services";
import { rabbitProxy } from "./rabbit-proxy";

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
