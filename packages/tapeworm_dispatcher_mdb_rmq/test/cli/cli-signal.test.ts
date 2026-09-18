import { EventEmitter } from "node:events";
import { expect, test } from "vitest";
import { runCli } from "../../bin/lifecycle";

test("SIGTERM initiates client cleanup while start remains pending and stop rejects", async () => {
  const signals = new EventEmitter<{ SIGINT: []; SIGTERM: [] }>();
  const calls: string[] = [];
  const failure = new Error("Stop failed");
  let finishStart = () => {};
  const start = new Promise<void>((resolve) => { finishStart = resolve; });
  const running = runCli({ close: () => { calls.push("close"); return Promise.resolve(); } },
    () => Promise.resolve({ start: () => start,
      stop: () => { calls.push("stop"); return Promise.reject(failure); } }), signals);
  const rejected = expect(running).rejects.toBe(failure);
  try {
    await new Promise<void>((resolve) => setImmediate(resolve));
    signals.emit("SIGTERM");
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect([...calls]).toEqual(["stop", "close"]);
  } finally { finishStart(); await rejected; }
  expect(signals.eventNames()).toEqual([]);
});
