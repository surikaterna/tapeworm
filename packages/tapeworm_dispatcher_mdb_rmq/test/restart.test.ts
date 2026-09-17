import { expect, test } from "vitest";
import { RecoveryWatcher } from "../src/recovery-watcher";
import { HistoryExpired } from "../src/recovery-errors";
import { commit, FakeHistory, FakeLive, item, stamp, state, token } from "./fixtures";
import type { ResumeState } from "../src/types";

test("replay restart keeps inclusive acknowledged scan position and original boundary", async () => {
  const history = new FakeHistory(); history.commits = [commit(11), commit(12)];
  const live = new FakeLive(async function* () { await Promise.resolve(); yield* []; throw new HistoryExpired(); });
  const first = new RecoveryWatcher(live, history, { retryDelayMs: 0, maxRetries: 1 });
  let saved: ResumeState = state();
  await first.connect();
  await expect(first.startWithProgress(saved, (value, progress) => {
    if (value?.id === "commit-12") return Promise.reject(new Error("Crash"));
    saved = progress.state; return Promise.resolve();
  })).rejects.toThrow("Crash");
  expect(saved.recovery?.cursor).toBe(token(11));
  const nextHistory = new FakeHistory(); nextHistory.commits = [commit(11), commit(12)];
  const second = new RecoveryWatcher(new FakeLive(async function* () { await Promise.resolve(); yield item(13); }), nextHistory, {});
  await second.connect();
  await second.startWithProgress(saved, async (_, progress) => { if (progress.kind === "live") await second.stop(); });
  expect(nextHistory.calls).toBe(0);
  expect(nextHistory.scans[0]?.cursor).toBe(token(11));
  expect(nextHistory.scans[0]?.boundary).toEqual(stamp);
});

test("save-failed fresh boundary is retried unchanged, and repeated starts reject", async () => {
  const history = new FakeHistory();
  const live = new FakeLive(async function* () { await Promise.resolve(); yield item(11); });
  const watcher = new RecoveryWatcher(live, history, { retryDelayMs: 0 });
  watcher.on("error", () => {});
  let transitions = 0;
  await watcher.connect();
  await watcher.startWithProgress(null, async (_, progress) => {
    if (progress.kind === "transition" && ++transitions === 1) throw new Error("Save unavailable");
    if (progress.kind !== "live") return;
    await expect(watcher.startWithProgress(null, () => Promise.resolve())).rejects.toThrow("already running");
    await watcher.stop();
  });
  expect(history.calls).toBe(1);
  expect(transitions).toBe(2);
});
