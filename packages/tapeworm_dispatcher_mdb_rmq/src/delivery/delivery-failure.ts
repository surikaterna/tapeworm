import type { ICommit } from "tapeworm";

export interface DeliveryFailureContext {
  readonly commit: Readonly<ICommit>;
  readonly feed: string;
  readonly collection: string;
}

export type DeliveryFailureResult = { kind: "unhandled" }
  | { kind: "durablyHandled"; onCheckpointed?: () => undefined };

/** Trusted extension: durable acceptance must complete before returning a receipt. */
export interface DeliveryFailureHandler {
  handle(error: unknown, context: DeliveryFailureContext): Promise<DeliveryFailureResult>;
}

export type FailureReceipt = { readonly kind: "unhandled" }
  | { readonly kind: "durablyHandled"; readonly onCheckpointed?: () => unknown };

function callable(value: unknown): value is () => unknown { return typeof value === "function"; }

/** Snapshot handler-owned accessors once, before checkpoint persistence can change their values. */
export function snapshotFailureResult(value: unknown): FailureReceipt {
  if (!value || typeof value !== "object" || !("kind" in value)) {
    throw new Error("Invalid delivery failure receipt");
  }
  const kind = value.kind;
  if (kind !== "unhandled" && kind !== "durablyHandled") {
    throw new Error("Invalid delivery failure receipt kind");
  }
  if (kind === "unhandled") {
    if ("onCheckpointed" in value) throw new Error("Invalid delivery failure notification");
    return { kind };
  }
  const onCheckpointed = "onCheckpointed" in value ? value.onCheckpointed : undefined;
  if (onCheckpointed !== undefined && !callable(onCheckpointed)) {
    throw new Error("Invalid delivery failure notification");
  }
  return { kind, onCheckpointed };
}
