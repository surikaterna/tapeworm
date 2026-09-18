import { expect, test } from "vitest";
import { deliver, type PublisherPort } from "../../src/delivery/delivery";
import { commit, MemoryStore, state } from "../support/fixtures";
import type { DurableProgress } from "../../src/types";
import type { IResumeTokenStore } from "../../src/checkpoints/types";

const progress = (): DurableProgress => ({ kind: "replay", state: state() });

test("four poison failures never checkpoint, then success checkpoints", async () => {
  const store = new MemoryStore();
  let failure = true;
  const publisher: PublisherPort = { connect: () => Promise.resolve(), close: () => Promise.resolve(),
    publish: () => failure ? Promise.reject(new Error("Poison")) : Promise.resolve() };
  for (let i = 0; i < 4; i++) {
    await expect(deliver(commit(11), progress(), publisher, store, "commits", "feed")).rejects.toThrow("Poison");
  }
  expect(store.saved).toEqual([]);
  failure = false;
  await deliver(commit(11), progress(), publisher, store, "commits", "feed");
  expect(store.saved).toHaveLength(1);
});

test("save failure duplicates stable identity, never pretends success", async () => {
  const store = new MemoryStore();
  const ids: string[] = [];
  const publisher: PublisherPort = { connect: () => Promise.resolve(), close: () => Promise.resolve(),
    publish: (value) => { ids.push(value.id); return Promise.resolve(); } };
  store.failure = true;
  await expect(deliver(commit(11), progress(), publisher, store, "commits", "feed")).rejects.toThrow("Save failed");
  expect(store.saved).toHaveLength(0);
  store.failure = false;
  await deliver(commit(11), progress(), publisher, store, "commits", "feed");
  expect(ids).toEqual(["commit-11", "commit-11"]);
});

test("custom three-field stores fail transition roundtrip instead of silently losing recovery", async () => {
  const memory = new MemoryStore();
  const store: IResumeTokenStore = { load: () => memory.load(), save: (value) => memory.save({
    updatedAt: value.updatedAt, lastCommitToken: value.lastCommitToken, changeStreamToken: value.changeStreamToken,
  }) };
  const publisher: PublisherPort = { connect: () => Promise.resolve(), close: () => Promise.resolve(), publish: () => Promise.resolve() };
  await expect(deliver(undefined, { kind: "transition", state: { ...state(), version: 1 } }, publisher, store, "commits", "feed"))
    .rejects.toThrow("roundtrip");
  await deliver(undefined, { kind: "transition", state: { ...state(), version: 1 } }, publisher, memory, "commits", "feed");
});
