import { describe, it, expect } from "vitest";
import {
  getLastAuthorizedCommitSequence,
  getLastCommitSequence,
} from "../src/client/util";
import type { ICommit } from "tapeworm";

type TestCommit = Partial<ICommit> & {
  id: string;
  commitSequence: number;
  authorative?: boolean;
};

function asCommits(arr: TestCommit[]): ICommit[] {
  return arr as unknown as ICommit[];
}

describe("Util", function () {
  it("#getLastAuthorizedCommitSequence should return -1 for no authorative commit", function () {
    var seq = getLastAuthorizedCommitSequence(
      asCommits([{ id: "1", commitSequence: 0 } as TestCommit]),
    );
    expect(seq).toBe(-1);
  });
  it("#getLastAuthorizedCommitSequence should return -1 for no authorative commits", function () {
    var seq = getLastAuthorizedCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0 } as TestCommit,
        { id: "1", commitSequence: 1 } as TestCommit,
      ]),
    );
    expect(seq).toBe(-1);
  });
  it("#getLastAuthorizedCommitSequence should return 0 for if first commit is authorative", function () {
    var seq = getLastAuthorizedCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0, authorative: true } as TestCommit,
        { id: "1", commitSequence: 1 } as TestCommit,
      ]),
    );
    expect(seq).toBe(0);
  });
  it("#getLastAuthorizedCommitSequence should return 1 for if first commit is authorative", function () {
    var seq = getLastAuthorizedCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0, authorative: true } as TestCommit,
        { id: "1", commitSequence: 1, authorative: true } as TestCommit,
      ]),
    );
    expect(seq).toBe(1);
  });
  it("#getLastCommitSequence should work with one element", function () {
    var seq = getLastCommitSequence(
      asCommits([{ id: "1", commitSequence: 0 } as TestCommit]),
    );
    expect(seq).toBe(0);
  });
  it("#getLastCommitSequence should work with two elements", function () {
    var seq = getLastCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0 } as TestCommit,
        { id: "1", commitSequence: 1 } as TestCommit,
      ]),
    );
    expect(seq).toBe(1);
  });
  it("#getLastCommitSequence should rwork with two elements with auth flag", function () {
    var seq = getLastCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0, authorative: true } as TestCommit,
        { id: "1", commitSequence: 1 } as TestCommit,
      ]),
    );
    expect(seq).toBe(1);
  });
  it("#getLastCommitSequence should rwork with two elements with multiple auth flag", function () {
    var seq = getLastCommitSequence(
      asCommits([
        { id: "1", commitSequence: 0, authorative: true } as TestCommit,
        { id: "1", commitSequence: 1, authorative: true } as TestCommit,
      ]),
    );
    expect(seq).toBe(1);
  });
  it("#getLastCommitSequence should return -1 if null commits", function () {
    var seq = getLastCommitSequence(null);
    expect(seq).toBe(-1);
  });
  it("#getLastCommitSequence should return -1 if 0 commits", function () {
    var seq = getLastCommitSequence([]);
    expect(seq).toBe(-1);
  });
});
