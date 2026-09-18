import type { ICommit } from "tapeworm";
import type { DurableProgress, ResumeState } from "../types";
import type { IResumeTokenStore } from "../checkpoints/types";
import { isDeepStrictEqual } from "node:util";
import { decodeState } from "../validation";
import { snapshotFailureResult, type DeliveryFailureHandler, type FailureReceipt } from "../delivery-failure";
import { DeliveryHalted } from "../delivery-halted";

export interface PublisherPort {
  connect(): Promise<void>;
  publish(commit: ICommit, collection: string): Promise<void>;
  close(): Promise<void>;
}

export async function deliver(commit: ICommit | undefined, progress: DurableProgress,
  publisher: PublisherPort, store: IResumeTokenStore, collection: string, feed: string): Promise<ResumeState> {
  return (await deliverOutcome(commit, progress, { publisher, store, collection, feed })).state;
}

export type DeliveryOutcome = { kind: "dispatched" | "handled" | "transition"; state: ResumeState };
export interface DeliveryOptions {
  publisher: PublisherPort;
  store: IResumeTokenStore;
  collection: string;
  feed: string;
  failureHandler?: DeliveryFailureHandler;
}

export async function deliverOutcome(commit: ICommit | undefined, progress: DurableProgress,
  options: DeliveryOptions): Promise<DeliveryOutcome> {
  const receipt = commit ? await publish(commit, options) : undefined;
  const state = await checkpoint(progress, options.store, options.feed);
  if (receipt?.kind === "durablyHandled") {
    notifyCheckpointed(receipt.onCheckpointed);
    return { kind: "handled", state };
  }
  return { kind: commit ? "dispatched" : "transition", state };
}

async function publish(commit: ICommit, options: DeliveryOptions): Promise<FailureReceipt | undefined> {
  const { publisher, collection, feed, failureHandler } = options;
  try { await publisher.publish(commit, collection); }
  catch (error: unknown) {
    if (!failureHandler) throw error;
    const receipt = snapshotFailureResult(await failureHandler.handle(error, { commit, collection, feed }));
    if (receipt.kind === "unhandled") throw error;
    return receipt;
  }
  return undefined;
}

function notifyCheckpointed(callback: (() => unknown) | undefined): void {
  if (!callback) return;
  try {
    const returned: unknown = callback();
    if (returned !== undefined) {
      void Promise.resolve(returned).catch(() => undefined);
      throw new Error("Delivery notification must synchronously return undefined");
    }
  } catch (cause: unknown) {
    throw new DeliveryHalted("Delivery notification failed after checkpoint; restart to reload durable progress", { cause });
  }
}

async function checkpoint(progress: DurableProgress, store: IResumeTokenStore, feed: string): Promise<ResumeState> {
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
