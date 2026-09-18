import { EventEmitter } from "node:events";
import type { DeliveryFailureContext, DeliveryFailureHandler, DeliveryFailureResult } from "../delivery/delivery-failure";
import { DeliveryHalted } from "../delivery/delivery-halted";
import { checkpointFeed } from "../checkpoints/feed";
import { rejectionCode } from "../rabbitmq/encoding";
import type { DispatcherConfig } from "../types";
import { QuarantinePaused } from "./errors";
import { MongoQuarantineSourceReader } from "./source-reader";
import type { QuarantineConfig, QuarantineRecord, QuarantineScope, QuarantinedEvent } from "./types";
import { assertScope, sourceReference, validateQuarantine } from "./validation";

export type QuarantineFailureHandlerOptions = Pick<DispatcherConfig, "mongodb" | "rabbitmq" | "watchMode" | "tenant" | "feedId">
  & { quarantine: QuarantineConfig };
export interface QuarantineHandlerEvents { quarantined: [event: QuarantinedEvent] }

export class QuarantineFailureHandler extends EventEmitter<QuarantineHandlerEvents> implements DeliveryFailureHandler {
  private readonly scope: QuarantineScope;
  private readonly config: QuarantineConfig;
  private readonly db: DispatcherConfig["mongodb"]["db"];
  private readiness?: Promise<void>;

  constructor(options: QuarantineFailureHandlerOptions) {
    super();
    this.scope = { feed: checkpointFeed(options), sourceCollection: options.mongodb.collection };
    validateQuarantine(options.quarantine, this.scope);
    this.config = { ...options.quarantine };
    this.db = options.mongodb.db;
  }

  async handle(error: unknown, context: DeliveryFailureContext): Promise<DeliveryFailureResult> {
    const code = rejectionCode(error);
    if (!this.config.enabled || !code) return { kind: "unhandled" };
    assertScope({ feed: context.feed, sourceCollection: context.collection }, this.scope);
    await this.ready();
    const reference = sourceReference(context.commit, this.scope);
    const captured = await this.config.store.capture(reference, code);
    return this.accept(captured, this.config.mode ?? "continue");
  }

  private ready(): Promise<void> {
    this.readiness ??= this.initialize().catch((error: unknown) => {
      this.readiness = undefined;
      throw error;
    });
    return this.readiness;
  }

  private async initialize(): Promise<void> {
    if (!this.config.enabled) throw new Error("Quarantine is disabled");
    await new MongoQuarantineSourceReader(this.db, this.scope.sourceCollection, this.scope.feed).initialize();
    await this.config.store.initialize();
  }

  private accept(captured: QuarantineRecord, mode: "pause" | "continue"): DeliveryFailureResult {
    const resolved = captured.status === "published";
    const event: QuarantinedEvent = { id: captured.id, code: captured.code,
      checkpointAdvanced: false, resolution: resolved ? "published" : "unresolved" };
    if (!resolved && mode === "pause") {
      try { this.emit("quarantined", event); }
      catch (cause: unknown) { throw new DeliveryHalted("Quarantine pause notification failed", { cause }); }
      throw new QuarantinePaused(event);
    }
    return { kind: "durablyHandled", onCheckpointed: () => {
      this.emit("quarantined", { ...event, checkpointAdvanced: true });
    } };
  }
}
