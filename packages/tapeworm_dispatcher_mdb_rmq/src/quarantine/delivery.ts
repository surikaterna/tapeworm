import type { ICommit } from "tapeworm";
import { deliver, type PublisherPort } from "../delivery";
import { rejectionCode } from "../publication-policy";
import type { IResumeTokenStore } from "../resume/types";
import type { DurableProgress, ResumeState } from "../types";
import { sourceReference } from "./validation";
import { QuarantinePaused } from "./errors";
import type { QuarantineConfig, QuarantineRecord, QuarantinedEvent } from "./types";

export type DeliveryOutcome = { kind: "dispatched" | "transition"; state: ResumeState }
  | { kind: "quarantined"; state: ResumeState; event: QuarantinedEvent };
export interface DeliveryOptions {
  publisher: PublisherPort;
  store: IResumeTokenStore;
  collection: string;
  feed: string;
  quarantine?: QuarantineConfig;
}
export async function deliverOutcome(commit: ICommit | undefined, progress: DurableProgress,
  options: DeliveryOptions): Promise<DeliveryOutcome> {
  const { publisher, store, collection, feed, quarantine } = options;
  if (!commit || !quarantine?.enabled) {
    return { kind: commit ? "dispatched" : "transition", state: await deliver(commit, progress, publisher, store, collection, feed) };
  }
  const reference = sourceReference(commit, { feed, sourceCollection: collection });
  let captured = await quarantine.store.find(reference);
  if (!captured) {
    try { await publisher.publish(commit, collection); }
    catch (error: unknown) {
      const code = rejectionCode(error);
      if (!code) throw error;
      captured = await quarantine.store.capture(reference, code);
    }
  }
  if (!captured) return { kind: "dispatched", state: await deliver(undefined, progress, publisher, store, collection, feed) };
  return acceptQuarantine(captured, progress, options, quarantine.mode ?? "continue");
}
async function acceptQuarantine(captured: QuarantineRecord, progress: DurableProgress,
  options: DeliveryOptions, mode: "pause" | "continue"): Promise<DeliveryOutcome> {
  const resolved = captured.status === "published";
  const event: QuarantinedEvent = { id: captured.id, code: captured.code,
    checkpointAdvanced: false, resolution: resolved ? "published" : "unresolved" };
  if (!resolved && mode === "pause") throw new QuarantinePaused(event);
  const { publisher, store, collection, feed } = options;
  const state = await deliver(undefined, progress, publisher, store, collection, feed);
  return { kind: "quarantined", state, event: { ...event, checkpointAdvanced: true } };
}
