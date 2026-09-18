import type { ICommit } from "tapeworm";
import type { PublisherPort } from "../delivery/delivery";
import { checkpointFeed } from "../checkpoints/feed";
import { encodePublication, rejectionCode, validatePublicationPolicy } from "../rabbitmq/encoding";
import type { DispatcherConfig } from "../types";
import type { AttemptResult, ClaimResult, DiagnosticCode, QuarantineListOptions, QuarantinePage, QuarantineSourceReader,
  QuarantineStore, RedriveOutcome, RedriveRequest, SourceReference } from "./types";
import { assertScope, validateRequest, validateRetention } from "./validation";

export interface QuarantineServiceOptions extends Pick<DispatcherConfig,
  "mongodb" | "rabbitmq" | "feedId" | "watchMode" | "tenant" | "publication"> {
  store: QuarantineStore;
  source: QuarantineSourceReader;
  sourceRetention: "immutable-until-resolved";
  /** Caller owns this publisher's lifetime and must bind it to the configured destination. */
  publisher: PublisherPort;
}
export class QuarantineService {
  private readonly active = new Set<Promise<RedriveOutcome>>();
  private ready?: Promise<void>;
  private closed = false;
  constructor(private readonly options: QuarantineServiceOptions) {
    const scope = { feed: checkpointFeed(options), sourceCollection: options.mongodb.collection };
    assertScope(options.store.scope, scope);
    assertScope(options.source.scope, scope);
    validateRetention(options.sourceRetention);
    validatePublicationPolicy(options.publication);
  }
  private initialize(): Promise<void> {
    if (this.closed) return Promise.reject(new Error("Quarantine service closed"));
    this.ready ??= Promise.all([this.options.store.initialize(), this.options.source.initialize()]).then(() => {});
    return this.ready;
  }
  async list(options?: QuarantineListOptions): Promise<QuarantinePage> {
    if (this.closed) throw new Error("Quarantine service closed");
    await this.options.store.initialize();
    return this.options.store.list(options);
  }
  redrive(id: string, request: RedriveRequest): Promise<RedriveOutcome> {
    validateRequest(request);
    if (this.closed) return Promise.reject(new Error("Quarantine service closed"));
    const operation = this.attempt(id, request);
    this.active.add(operation);
    void operation.then(() => this.active.delete(operation), () => this.active.delete(operation));
    return operation;
  }
  private async attempt(id: string, request: RedriveRequest): Promise<RedriveOutcome> {
    await this.initialize();
    let claim: ClaimResult;
    try { claim = await this.options.store.claim(id, request); }
    catch { return { kind: "outcome-unknown" }; }
    if (claim.kind === "attempt-limit") return { kind: "rejected", code: "attempt-limit" };
    if (claim.kind !== "claimed") return { kind: claim.kind };
    const prepared = await this.prepare(claim.record.reference);
    if (prepared.kind === "rejected") return this.complete(id, claim.token, "rejected", prepared.code);
    try {
      await this.options.publisher.connect();
      await this.options.publisher.publish(prepared.commit, this.options.mongodb.collection);
    } catch { return this.complete(id, claim.token, "outcome-unknown", "publication-failed"); }
    return this.complete(id, claim.token, "published");
  }
  private async prepare(reference: SourceReference): Promise<{ kind: "ready"; commit: ICommit }
    | { kind: "rejected"; code: DiagnosticCode }> {
    let commit: ICommit;
    try { commit = await this.options.source.read(reference); }
    catch { return { kind: "rejected", code: "source-invalid" }; }
    try { encodePublication(commit, this.options.publication); }
    catch (error: unknown) { return { kind: "rejected", code: rejectionCode(error) ?? "publication-failed" }; }
    return { kind: "ready", commit };
  }
  private async complete(id: string, token: string, result: AttemptResult, code?: DiagnosticCode): Promise<RedriveOutcome> {
    try {
      if (!await this.options.store.finish(id, token, result, code)) return { kind: "outcome-unknown" };
    } catch { return { kind: "outcome-unknown" }; }
    if (result === "rejected") return { kind: "rejected", code: code ?? "publication-failed" };
    return { kind: result };
  }
  /** Drain explicit attempts; the caller closes its publisher after this resolves. */
  async close(): Promise<void> {
    this.closed = true;
    await Promise.allSettled([...this.active]);
  }
}
