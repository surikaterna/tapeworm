import { EventEmitter } from "node:events";
import { runCli, type CliShutdownTimeout, type CliLifecycleOptions } from "../bin/lifecycle";

export function deferred() {
  let resolve = () => {};
  let reject: (error: unknown) => void = () => {};
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

export function shutdownFixture(stop = () => Promise.resolve(), close = () => Promise.resolve(),
  options: CliLifecycleOptions = {}) {
  const signals = new EventEmitter<{ SIGINT: []; SIGTERM: [] }>();
  const start = deferred();
  const calls: string[] = [];
  const deadlines: CliShutdownTimeout[] = [];
  const late: unknown[] = [];
  const running = runCli({ close: () => { calls.push("close"); return close(); } },
    () => Promise.resolve({ start: () => { calls.push("start"); return start.promise; },
      stop: () => { calls.push("stop"); return stop(); } }), signals, {
      shutdownTimeoutMs: 100,
      onDeadline: (error) => { calls.push("deadline"); deadlines.push(error); },
      onLateError: (error) => { late.push(error); }, ...options,
    });
  return { signals, start, calls, deadlines, late, running };
}
