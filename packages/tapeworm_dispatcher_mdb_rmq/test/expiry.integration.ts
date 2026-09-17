import { expect, test } from "vitest";
import { ChangeStreamWatcher, OplogWatcher } from "../index";
import { ChangeStreamSource, OplogSource } from "../src/live-source";
import { HistoryExpired } from "../src/recovery-errors";
import { expiredMongo } from "./expired-mongo";
import { commit, token } from "./fixtures";
import type { DurableProgress } from "../src/types";

test("actual oplog rollover expires a real resume token and activates bounded recovery", async () => {
  const env = await expiredMongo();
  const watcher = new ChangeStreamWatcher(env.config);
  try {
    const progress: DurableProgress[] = [];
    let fallbacks = 0;
    watcher.on("fallback", () => { fallbacks++; });
    watcher.on("error", () => {});
    await watcher.connect();
    await watcher.startWithProgress({ updatedAt: new Date(), lastCommitToken: token(10),
      primary: env.primary }, async (_, value) => {
      progress.push(value);
      if (value.kind === "transition" && value.state.recovery?.phase === "cutover") {
        await env.db.collection("commits").insertOne(commit(5));
      }
      if (value.kind === "live" && value.state.lastCommitToken === token(5)) await watcher.stop();
    });
    expect(fallbacks).toBe(1);
    expect(progress.some((value) => value.kind === "replay")).toBe(true);
    expect(progress.at(-1)?.state.lastCommitToken).toBe(token(5));
  } finally { await watcher.stop(); await env.close(); }
});

test("real source modes report actual rolled-off server boundary", async () => {
  const env = await expiredMongo();
  const sources = [new ChangeStreamSource(env.config), new OplogSource(env.config)];
  try {
    for (const source of sources) {
      const cursor = source.watch({ kind: "boundary", mode: source.mode, ts: env.boundary })[Symbol.asyncIterator]();
      await expect(cursor.next()).rejects.toBeInstanceOf(HistoryExpired);
      await cursor.return?.();
    }
  } finally { await Promise.all(sources.map((source) => source.close())); await env.close(); }
});

test.each(["changeStream", "oplog"] as const)("persisted cutover boundary expiry stops without reset in %s", async (mode) => {
  const env = await expiredMongo();
  const watcher = mode === "changeStream" ? new ChangeStreamWatcher(env.config) : new OplogWatcher(env.config);
  let calls = 0;
  let fallbacks = 0;
  watcher.on("fallback", () => { fallbacks++; });
  try {
    await watcher.connect();
    await expect(watcher.startWithProgress({ version: 1, updatedAt: new Date(), lastCommitToken: token(10),
      primary: { kind: "boundary", mode, ts: env.boundary },
      recovery: { phase: "cutover", lower: token(10), upper: token(20), boundary: env.boundary, startedAt: new Date() },
    }, () => { calls++; return Promise.resolve(); })).rejects.toThrow("operator action required");
    expect(calls).toBe(0); expect(fallbacks).toBe(0);
  } finally { await watcher.stop(); await env.close(); }
});
