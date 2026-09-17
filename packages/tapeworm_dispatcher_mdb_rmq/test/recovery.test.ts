import { expect, test } from "vitest";
import { RecoveryWatcher } from "../src/recovery-watcher";
import { HistoryExpired, isHistoryExpired } from "../src/recovery-errors";
import { commit, FakeHistory, FakeLive, item, stamp, state, token } from "./fixtures";
import type { DurableProgress } from "../src/types";

function setup(live: FakeLive, history = new FakeHistory(), maxRetries = 4) {
  const watcher = new RecoveryWatcher(live, history, { retryDelayMs: 0, maxRetries });
  watcher.on("error", () => {});
  return { watcher, history };
}

test("retry starts at latest acknowledged primary, not startup snapshot", async () => {
  const live = new FakeLive(async function* (attempt) {
    await Promise.resolve();
    if (attempt === 1) { yield item(11); throw new Error("Network"); }
    yield item(12);
  });
  const { watcher, history } = setup(live);
  await watcher.connect();
  await watcher.startWithProgress(state(), async (value) => { if (value?.id === "commit-12") await watcher.stop(); });
  expect(live.positions[1]).toEqual(item(11).position);
  expect(history.calls).toBe(0);
});

test("handler error that looks like Mongo 286 never activates fallback", async () => {
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(11); });
  const { watcher, history } = setup(live);
  let calls = 0;
  await watcher.connect();
  await expect(watcher.startWithProgress(state(), () => {
    calls++; return Promise.reject(Object.assign(new Error("Publish failed"), { code: 286 }));
  })).rejects.toThrow("Publish failed");
  expect(calls).toBe(4);
  expect(history.calls).toBe(0);
  expect(live.positions.every((position) => JSON.stringify(position) === JSON.stringify(state().primary))).toBe(true);
});

test.each([13, 18, 280, 6])("Mongo code %i is not automatically history expiry", (code) => {
  expect(isHistoryExpired({ code })).toBe(false);
});

test("expiry captures boundary, saves replay separately, and durably cuts over", async () => {
  const live = new FakeLive(async function* (attempt) {
    await Promise.resolve();
    if (attempt === 1) throw new HistoryExpired("expired");
    yield item(12);
  });
  const { watcher, history } = setup(live);
  history.commits = [commit(11)];
  const progress: DurableProgress[] = [];
  await watcher.connect();
  await watcher.startWithProgress(state(), async (_, next) => {
    progress.push(next);
    if (next.kind === "live") await watcher.stop();
  });
  expect(progress.map((next) => next.kind)).toEqual(["transition", "replay", "transition", "live"]);
  expect(progress[1]?.state.primary).toEqual(state().primary);
  expect(progress[1]?.state.recovery?.cursor).toBe(token(11));
  expect(live.positions[1]).toEqual({ kind: "boundary", mode: "changeStream", ts: stamp });
});

test("empty recovery persists cutover and expired boundary halts instead of resetting", async () => {
  const live = new FakeLive(async function* () { await Promise.resolve(); yield* []; throw new HistoryExpired("expired"); });
  const { watcher, history } = setup(live);
  const progress: DurableProgress[] = [];
  await watcher.connect();
  await expect(watcher.startWithProgress(state(), (_, next) => {
    progress.push(next); return Promise.resolve();
  })).rejects.toThrow("operator action required");
  expect(progress.map((next) => next.state.recovery?.phase)).toEqual(["scan", "cutover"]);
  expect(history.calls).toBe(1);
});

test("legacy Document callback fails clearly rather than saving fake replay token", async () => {
  const { watcher } = setup(new FakeLive(async function* () { await Promise.resolve(); yield item(11); }));
  await watcher.connect();
  await expect(watcher.start({ updatedAt: new Date(), lastCommitToken: token(10) }, () => Promise.resolve()))
    .rejects.toThrow("Legacy handler cannot persist recovery");
});

test("legacy UUID recovery is explicitly not an expiry event", async () => {
  const { watcher } = setup(new FakeLive(async function* () { await Promise.resolve(); yield item(11); }));
  let fallbacks = 0;
  watcher.on("fallback", () => { fallbacks++; });
  await watcher.connect();
  await watcher.startWithProgress({ updatedAt: new Date(), lastCommitToken: token(10) }, async (_, progress) => {
    if (progress.kind === "live") await watcher.stop();
  });
  expect(fallbacks).toBe(0);
});
