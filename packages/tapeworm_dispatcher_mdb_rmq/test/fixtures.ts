import { Timestamp, UUID } from "mongodb";
import type { ICommit } from "tapeworm";
import type { HistoryPort } from "../src/history";
import type { LiveItem, LivePort } from "../src/live-source";
import type { PrimaryPosition, RecoveryPosition, ResumeState, WatchMode } from "../src/types";
import type { IResumeTokenStore } from "../src/resume/types";

export const token = (n: number) => `00000000-0000-7000-8000-${n.toString().padStart(12, "0")}`;
export const stamp = new Timestamp({ t: 100, i: 1 });
export const commit = (n: number): ICommit => ({ id: `commit-${n}`, partitionId: "master",
  streamId: "stream", commitSequence: n, token: new UUID(token(n)), events: [{ id: `event-${n}`, type: "test", payload: { n } }] });
export const item = (n: number): LiveItem => ({ commit: commit(n), position: { kind: "changeStream", token: { _data: String(n) } } });
export const state = (): ResumeState => ({ updatedAt: new Date(), lastCommitToken: token(10),
  primary: { kind: "changeStream", token: { _data: "10" } } });

export class MemoryStore implements IResumeTokenStore {
  saved: ResumeState[] = [];
  failure = false;
  load(): Promise<ResumeState | null> { return Promise.resolve(this.saved.at(-1) ?? null); }
  save(value: ResumeState): Promise<void> {
    if (this.failure) return Promise.reject(new Error("Save failed"));
    this.saved.push(value);
    return Promise.resolve();
  }
}

export class FakeHistory implements HistoryPort {
  calls = 0;
  scans: RecoveryPosition[] = [];
  commits: ICommit[] = [];
  boundary(): Promise<Timestamp> { this.calls++; return Promise.resolve(stamp); }
  upper(): Promise<string | undefined> { return Promise.resolve(token(30)); }
  async *scan(position: RecoveryPosition): AsyncIterable<ICommit, void, unknown> {
    await Promise.resolve();
    this.scans.push(position);
    for (const value of this.commits) yield value;
  }
  close(): Promise<void> { return Promise.resolve(); }
}

export class FakeLive implements LivePort {
  readonly mode: WatchMode = "changeStream";
  positions: PrimaryPosition[] = [];
  constructor(private readonly sequence: (attempt: number) => AsyncIterable<LiveItem, void, unknown>) {}
  watch(position: PrimaryPosition): AsyncIterable<LiveItem, void, unknown> {
    this.positions.push(position);
    return this.sequence(this.positions.length);
  }
  close(): Promise<void> { return Promise.resolve(); }
}
