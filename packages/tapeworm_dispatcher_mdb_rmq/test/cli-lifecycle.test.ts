import { EventEmitter } from "node:events";
import { expect, test } from "vitest";
import { runCli, type CliClient, type CliDispatcher, type CliSignals } from "../bin/lifecycle";

interface Failures { initialize?: Error; start?: Error; stop?: Error; close?: Error }

function fixture(failures: Failures = {}) {
  const calls: string[] = [];
  const signals = new EventEmitter<{ SIGINT: []; SIGTERM: [] }>();
  const operation = (name: string, error?: Error): Promise<void> => {
    calls.push(name);
    return error ? Promise.reject(error) : Promise.resolve();
  };
  const client: CliClient = { close: () => operation("close", failures.close) };
  const dispatcher: CliDispatcher = {
    start: () => operation("start", failures.start), stop: () => operation("stop", failures.stop),
  };
  const initialize = async () => { await operation("initialize", failures.initialize); return dispatcher; };
  return { calls, signals, client, initialize };
}

test("initialization failure closes the acquired client and preserves the primary error", async () => {
  const primary = new Error("Initialization failed after connecting");
  const env = fixture({ initialize: primary });
  await expect(runCli(env.client, env.initialize, env.signals)).rejects.toBe(primary);
  expect(env.calls).toEqual(["initialize", "close"]);
  expect(env.signals.eventNames()).toEqual([]);
});

test("rejected stop cannot skip client closure or signal listener removal", async () => {
  const stop = new Error("Stop failed");
  const env = fixture({ stop });
  await expect(runCli(env.client, env.initialize, env.signals)).rejects.toBe(stop);
  expect(env.calls).toEqual(["initialize", "start", "stop", "close"]);
  expect(env.signals.eventNames()).toEqual([]);
});

test("rejected client close still removes signal listeners", async () => {
  const close = new Error("Close failed");
  const env = fixture({ close });
  await expect(runCli(env.client, env.initialize, env.signals)).rejects.toBe(close);
  expect(env.calls).toEqual(["initialize", "start", "stop", "close"]);
  expect(env.signals.eventNames()).toEqual([]);
});

test("teardown failures retain the startup failure as cause and all errors in order", async () => {
  const start = new Error("Primary startup failure");
  const stop = new Error("Stop failed");
  const close = new Error("Close failed");
  const env = fixture({ start, stop, close });
  await expect(runCli(env.client, env.initialize, env.signals)).rejects.toMatchObject({
    message: "CLI failed: Primary startup failure; additional cleanup failures",
    cause: start, errors: [start, stop, close],
  });
  expect(env.calls).toEqual(["initialize", "start", "stop", "close"]);
  expect(env.signals.eventNames()).toEqual([]);
});

test("successful run cleans up only its own signal listeners", async () => {
  const env = fixture();
  const existing = () => {};
  env.signals.on("SIGINT", existing);
  env.signals.on("SIGTERM", existing);
  await runCli(env.client, env.initialize, env.signals);
  expect(env.calls).toEqual(["initialize", "start", "stop", "close"]);
  expect(env.signals.listeners("SIGINT")).toEqual([existing]);
  expect(env.signals.listeners("SIGTERM")).toEqual([existing]);
});

test("partial signal initialization still closes resources and removes registered listeners", async () => {
  const env = fixture();
  const primary = new Error("Signal registration failed");
  const signals: CliSignals = {
    once: (signal, listener) => {
      if (signal === "SIGTERM") throw primary;
      return env.signals.once(signal, listener);
    },
    removeListener: (signal, listener) => env.signals.removeListener(signal, listener),
  };
  await expect(runCli(env.client, env.initialize, signals)).rejects.toBe(primary);
  expect(env.calls).toEqual(["close"]);
  expect(env.signals.eventNames()).toEqual([]);
});
