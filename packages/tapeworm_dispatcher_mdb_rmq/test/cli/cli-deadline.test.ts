import { expect, test, vi } from "vitest";
import { CliShutdownTimeout, runCli } from "../../bin/lifecycle";
import { deferred, shutdownFixture } from "./support/shutdown-fixture";
import { EventEmitter } from "node:events";

test.each(["success", "reject", "pending"])("pending start with %s stop remains deadline protected", async (mode) => {
  vi.useFakeTimers();
  try {
    const stop = deferred();
    const failure = new Error("Stop rejected");
    const env = shutdownFixture(() => stop.promise);
    const ended = expect(env.running).rejects.toBeInstanceOf(CliShutdownTimeout);
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    env.signals.emit("SIGTERM");
    if (mode === "success") stop.resolve();
    if (mode === "reject") stop.reject(failure);
    await vi.advanceTimersByTimeAsync(99);
    expect(env.calls).toEqual(mode === "reject" ? ["start", "stop", "close"] : ["start", "stop"]);
    expect(env.deadlines).toEqual([]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1); await ended;
    expect(env.calls).toEqual(["start", "stop", "close", "deadline"]);
    expect(env.deadlines[0]?.cause).toBe(mode === "reject" ? failure : undefined);
    expect(env.signals.eventNames()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});

test("a pending close cannot extend the absolute budget after start settles", async () => {
  vi.useFakeTimers();
  try {
    const close = deferred();
    const env = shutdownFixture(() => Promise.resolve(), () => close.promise);
    const ended = expect(env.running).rejects.toBeInstanceOf(CliShutdownTimeout);
    await vi.advanceTimersByTimeAsync(0);
    env.signals.emit("SIGTERM"); env.start.resolve();
    await vi.advanceTimersByTimeAsync(100); await ended;
    expect(env.calls).toEqual(["start", "stop", "close", "deadline"]);
    expect(env.signals.eventNames()).toEqual([]);
    const error = new Error("Late close failure"); close.reject(error);
    await vi.advanceTimersByTimeAsync(0);
    expect(env.late).toEqual([error]);
  } finally { vi.useRealTimers(); }
});

test("repeated SIGTERM/SIGINT neither restart the budget nor invoke cleanup twice", async () => {
  vi.useFakeTimers();
  try {
    const env = shutdownFixture();
    const ended = expect(env.running).rejects.toBeInstanceOf(CliShutdownTimeout);
    await vi.advanceTimersByTimeAsync(0); env.signals.emit("SIGTERM");
    await vi.advanceTimersByTimeAsync(50);
    env.signals.emit("SIGTERM"); env.signals.emit("SIGINT");
    expect(env.signals.listenerCount("SIGTERM")).toBe(1);
    expect(env.signals.listenerCount("SIGINT")).toBe(1);
    await vi.advanceTimersByTimeAsync(49); expect(env.deadlines).toEqual([]);
    await vi.advanceTimersByTimeAsync(1); await ended;
    expect(env.calls).toEqual(["start", "stop", "close", "deadline"]);
    expect(env.signals.eventNames()).toEqual([]);
  } finally { vi.useRealTimers(); }
});

test("start and cleanup finishing just before the deadline drain gracefully", async () => {
  vi.useFakeTimers();
  try {
    const stop = deferred(); const env = shutdownFixture(() => stop.promise);
    await vi.advanceTimersByTimeAsync(0); env.signals.emit("SIGTERM");
    await vi.advanceTimersByTimeAsync(99);
    expect(env.deadlines).toEqual([]);
    stop.resolve(); env.start.resolve(); await env.running;
    await vi.advanceTimersByTimeAsync(1);
    expect(env.calls).toEqual(["start", "stop", "close"]);
    expect(env.deadlines).toEqual([]);
    expect(env.signals.eventNames()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});

test("late start and stop rejections are observed and sent to the diagnostic sink", async () => {
  vi.useFakeTimers();
  try {
    const stop = deferred(); const env = shutdownFixture(() => stop.promise);
    const ended = expect(env.running).rejects.toBeInstanceOf(CliShutdownTimeout);
    await vi.advanceTimersByTimeAsync(0); env.signals.emit("SIGTERM");
    await vi.advanceTimersByTimeAsync(100); await ended;
    const startError = new Error("Late start"); const stopError = new Error("Late stop");
    env.start.reject(startError); stop.reject(stopError);
    await vi.advanceTimersByTimeAsync(0);
    expect(env.late).toEqual([startError, stopError]);
    expect(env.calls).toEqual(["start", "stop", "close", "deadline"]);
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});

test("synchronous primary failure remains the timeout cause when stop never settles", async () => {
  vi.useFakeTimers();
  try {
    const primary = new Error("Synchronous startup failure");
    const signals = new EventEmitter<{ SIGINT: []; SIGTERM: [] }>();
    const stop = deferred(); const calls: string[] = [];
    const running = runCli({ close: () => { calls.push("close"); return Promise.resolve(); } },
      () => Promise.resolve({ start: () => { throw primary; }, stop: () => stop.promise }), signals,
      { shutdownTimeoutMs: 100, onDeadline: () => { calls.push("deadline"); } });
    const ended = expect(running).rejects.toMatchObject({ name: "CliShutdownTimeout", cause: primary, failures: [primary] });
    await vi.advanceTimersByTimeAsync(100); await ended;
    expect(calls).toEqual(["close", "deadline"]);
    expect(signals.eventNames()).toEqual([]);
  } finally { vi.useRealTimers(); }
});

test("late failures of deadline and diagnostic callbacks do not become unhandled rejections", async () => {
  vi.useFakeTimers();
  try {
    const reported: unknown[] = [];
    const callbackError = new Error("Deadline callback rejected");
    const env = shutdownFixture(() => Promise.resolve(), () => Promise.resolve(), {
      onDeadline: () => Promise.reject(callbackError),
      onLateError: (error) => { reported.push(error); return Promise.reject(new Error("Sink rejected")); },
    });
    const ended = expect(env.running).rejects.toBeInstanceOf(CliShutdownTimeout);
    await vi.advanceTimersByTimeAsync(0); env.signals.emit("SIGTERM");
    await vi.advanceTimersByTimeAsync(100); await ended;
    expect(reported).toEqual([callbackError]);
  } finally { vi.useRealTimers(); }
});

test("synchronous stop and close throws are normalized without skipping listener cleanup", async () => {
  vi.useFakeTimers();
  try {
    const stop = new Error("Synchronous stop"); const close = new Error("Synchronous close");
    const env = shutdownFixture(() => { throw stop; }, () => { throw close; });
    const ended = expect(env.running).rejects.toMatchObject({ cause: stop, errors: [stop, close] });
    await vi.advanceTimersByTimeAsync(0); env.signals.emit("SIGTERM");
    expect(env.calls).toEqual(["start", "stop", "close"]);
    env.start.resolve(); await ended;
    expect(env.signals.eventNames()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});

test("signal during pending initialization closes Mongo independently and observes late failure", async () => {
  vi.useFakeTimers();
  try {
    const initialization = deferred();
    const signals = new EventEmitter<{ SIGINT: []; SIGTERM: [] }>();
    const calls: string[] = []; const late: unknown[] = [];
    const running = runCli({ close: () => { calls.push("close"); return Promise.resolve(); } }, async () => {
      await initialization.promise;
      return { start: () => Promise.resolve(), stop: () => Promise.resolve() };
    }, signals, { shutdownTimeoutMs: 100, onLateError: (error) => { late.push(error); } });
    const ended = expect(running).rejects.toBeInstanceOf(CliShutdownTimeout);
    signals.emit("SIGTERM");
    expect(calls).toEqual(["close"]);
    await vi.advanceTimersByTimeAsync(100); await ended;
    const failure = new Error("Late initialization"); initialization.reject(failure);
    await vi.advanceTimersByTimeAsync(0);
    expect(late).toEqual([failure]);
    expect(signals.eventNames()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});
