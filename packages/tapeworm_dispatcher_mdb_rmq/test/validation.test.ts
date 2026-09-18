import { expect, test } from "vitest";
import { decodeCommit, decodeState } from "../src/validation";
import { commit, stamp, state, token } from "./support/fixtures";

test("validates wire commit structure without interpreting domain payload", () => {
  expect(decodeCommit(commit(11))).toEqual(commit(11));
  expect(() => decodeCommit({ ...commit(11), events: [{ id: "id", type: 1 }] })).toThrow();
  expect(() => decodeCommit({ ...commit(11), commitSequence: "1" })).toThrow();
  const payload: unknown = { arbitrary: [null, "domain", 42] };
  expect(decodeCommit({ ...commit(11), events: [{ id: "id", type: "domain", payload }] }).events[0]?.payload).toEqual(payload);
});

test("checkpoint decoder rejects fake token, future version and malformed BSON", () => {
  expect(() => decodeState({ ...state(), changeStreamToken: { _replayFallback: true } })).toThrow("Synthetic");
  expect(() => decodeState({ ...state(), version: 2 })).toThrow("version");
  expect(() => decodeState({ ...state(), primary: { kind: "oplog", ts: 100 } })).toThrow("Timestamp");
  const value = { ...state(), lastCommitToken: token(11), version: 1, feed: "feed", recovery: { phase: "scan", lower: token(10),
    upper: token(20), cursor: token(11), boundary: stamp, startedAt: new Date() } };
  expect(decodeState(value)).toEqual(expect.objectContaining(value));
});

test("rejects inconsistent recovery rather than skipping a range", () => {
  const value = { ...state(), version: 1, recovery: { phase: "scan", lower: token(10),
    upper: token(20), cursor: token(30), boundary: stamp, startedAt: new Date() } };
  expect(() => decodeState(value)).toThrow("Inconsistent");
  expect(() => decodeState({ ...value, recovery: { ...value.recovery, cursor: undefined, phase: "cutover" } })).toThrow("original live boundary");
  expect(() => decodeState({ ...state(), primary: { kind: "changeStream", token: { _replayFallback: true } } })).toThrow("Synthetic");
});
