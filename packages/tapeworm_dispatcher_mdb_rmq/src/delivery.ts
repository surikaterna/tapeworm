import type { ICommit } from "tapeworm";
import type { DurableProgress, ResumeState } from "./types";
import type { IResumeTokenStore } from "./resume/types";
import { isDeepStrictEqual } from "node:util";
import { decodeState } from "./validation";

export interface PublisherPort {
  connect(): Promise<void>;
  publish(commit: ICommit, collection: string): Promise<void>;
  close(): Promise<void>;
}

export async function deliver(commit: ICommit | undefined, progress: DurableProgress,
  publisher: PublisherPort, store: IResumeTokenStore, collection: string, feed: string): Promise<ResumeState> {
  if (commit) await publisher.publish(commit, collection);
  const state = decodeState({ ...progress.state, feed });
  await store.save(state);
  if (progress.kind === "transition") {
    const loaded: unknown = await store.load();
    if (!isDeepStrictEqual(decodeState(loaded), state)) {
      throw new Error("Custom checkpoint store must durably roundtrip all versioned recovery fields and BSON values");
    }
  }
  return state;
}
