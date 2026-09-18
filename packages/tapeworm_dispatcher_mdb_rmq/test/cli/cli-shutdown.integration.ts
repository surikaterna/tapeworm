import { expect, test } from "vitest";
import { record } from "../../src/validation";
import { commit, token } from "../support/fixtures";
import { eventually } from "../support/services";
import { cleanupCli, exitWithin, launchCli, type CliProcess } from "./support/process";
import { lockedCheckpoint, messageIds } from "./support/lock-fixture";

test("locked checkpoint exits at the 10s deadline before unlock, then safely resumes", async () => {
  const env = await lockedCheckpoint();
  const children: CliProcess[] = [];
  let locked = false;
  try {
    await env.mongodb.db.admin().command({ fsync: 1, lock: true }); locked = true;
    const first = launchCli(env.mongodb.db.databaseName, env.mq.exchange); children.push(first);
    await env.waitBlocked();
    expect(await messageIds(env.mq)).toEqual(["commit-11"]);
    expect((await env.store.load())?.lastCommitToken).toBeUndefined();
    const signalledAt = Date.now(); first.child.kill("SIGTERM");
    const exit = await exitWithin(first, 15000);
    const elapsed = Date.now() - signalledAt;
    expect(exit).toEqual({ code: 124, signal: null });
    expect(elapsed).toBeGreaterThanOrEqual(9500); expect(elapsed).toBeLessThan(14500);
    expect(first.stderr()).toContain("CLI shutdown exceeded 10000ms; cleanup attempted");
    const current: unknown = await env.mongodb.db.admin().command({ currentOp: 1 });
    expect(record(current).fsyncLock).toBe(true);
    expect((await env.store.load())?.lastCommitToken).toBeUndefined();
    await env.mongodb.db.admin().command({ fsyncUnlock: 1 }); locked = false;
    await env.waitIdle();
    // Quiescent currentOp alone does not prove the old write is majority-visible.
    await env.mongodb.db.collection("shutdown_barrier").insertOne({}, { writeConcern: { w: "majority" } });
    const afterUnlock = (await env.store.load())?.lastCommitToken;
    expect([undefined, token(11)]).toContain(afterUnlock);
    await env.mongodb.db.collection("commits").insertOne(commit(12));
    const second = launchCli(env.mongodb.db.databaseName, env.mq.exchange); children.push(second);
    await eventually(async () => (await env.store.load())?.lastCommitToken === token(12));
    second.child.kill("SIGTERM");
    expect(await exitWithin(second, 5000)).toEqual({ code: 0, signal: null });
    const replayed = await messageIds(env.mq);
    expect(replayed).toContain("commit-12");
    expect(replayed.every((id) => id === "commit-11" || id === "commit-12")).toBe(true);
    if (afterUnlock === undefined) expect(replayed).toContain("commit-11");
    console.info({ shutdownMs: elapsed, exitCode: exit.code, lockedUntilExit: true, afterUnlock, replayed });
  } finally {
    try { await Promise.all(children.map(cleanupCli)); }
    finally {
      if (locked) await env.mongodb.db.admin().command({ fsyncUnlock: 1 });
      await Promise.all([env.mongodb.close(), env.mq.close()]);
    }
  }
});
