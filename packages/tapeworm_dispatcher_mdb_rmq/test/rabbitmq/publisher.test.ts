import { EventEmitter } from "node:events";
import { expect, test, vi } from "vitest";
import { ConfirmedChannel, type ConfirmPort, type PublishProperties } from "../../src/rabbitmq/confirmed-channel";

class Channel extends EventEmitter implements ConfirmPort {
  attempts: { options: PublishProperties; callback: (error: unknown) => void }[] = [];
  writable = true;
  publish(_exchange: string, _key: string, _content: Buffer, options: PublishProperties,
    callback: (error: unknown) => void): boolean {
    this.attempts.push({ options, callback }); return this.writable;
  }
  ack(index: number, error?: Error): void { this.attempts[index]?.callback(error); }
  returned(index: number): void { this.emit("return", { properties: this.attempts[index]?.options }); }
}

test("return before ACK rejects independently for concurrent identical commit IDs", async () => {
  const channel = new Channel();
  const confirmed = new ConfirmedChannel(channel, 1000, 2, () => {});
  const a = confirmed.publish("exchange", Buffer.from("a"), { messageId: "same" });
  const b = confirmed.publish("exchange", Buffer.from("b"), { messageId: "same" });
  const rejected = expect(a).rejects.toThrow("unroutable");
  channel.returned(0); channel.ack(0); channel.ack(1);
  await rejected; await b;
  expect(channel.attempts[0]?.options.mandatory).toBe(true);
  expect(channel.attempts[0]?.options.messageId).toBe("same");
  expect(channel.attempts[0]?.options.correlationId).not.toBe(channel.attempts[1]?.options.correlationId);
  confirmed.close();
  expect(channel.eventNames()).toEqual([]);
});

test("nack, bounded pending, backpressure and drain", async () => {
  const channel = new Channel();
  const confirmed = new ConfirmedChannel(channel, 1000, 1, () => {});
  channel.writable = false;
  const a = confirmed.publish("exchange", Buffer.from("a"), {});
  await expect(confirmed.publish("exchange", Buffer.from("b"), {})).rejects.toThrow("capacity");
  const rejected = expect(a).rejects.toThrow("negatively");
  channel.ack(0, new Error("nack")); await rejected;
  await expect(confirmed.publish("exchange", Buffer.from("b"), {})).rejects.toThrow("capacity");
  channel.writable = true; channel.emit("drain");
  const b = confirmed.publish("exchange", Buffer.from("b"), {});
  channel.ack(1); await b; confirmed.close();
});

test.each(["close", "error", "timeout", "stop"])("%s settles pending and cleans all listeners/timers", async (reason) => {
  vi.useFakeTimers();
  try {
    const channel = new Channel();
    let invalidated = 0;
    const confirmed = new ConfirmedChannel(channel, 1000, 2, () => { invalidated++; });
    const a = expect(confirmed.publish("exchange", Buffer.from("a"), {})).rejects.toThrow();
    const b = expect(confirmed.publish("exchange", Buffer.from("b"), {})).rejects.toThrow();
    if (reason === "timeout") await vi.advanceTimersByTimeAsync(1000);
    else if (reason === "stop") confirmed.close();
    else channel.emit(reason);
    await a; await b;
    expect(channel.eventNames()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    expect(invalidated).toBe(reason === "stop" ? 0 : 1);
  } finally { vi.useRealTimers(); }
});
