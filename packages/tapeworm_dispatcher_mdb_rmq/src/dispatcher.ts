import { EventEmitter } from "node:events";
import type { ICommit } from "tapeworm";
import type { DispatcherConfig, DispatcherEvents, DurableProgress, ResumeState } from "./types";
import { ChangeStreamWatcher } from "./watcher";
import type { DurableCommitWatcher } from "./watcher";
import { OplogWatcher } from "./oplog-watcher";
import { CommitPublisher } from "./publisher";
import { deliverOutcome } from "./delivery";
import { validateDispatcherConfig } from "./dispatcher-config";
import { decodeState } from "./validation";
import { checkpointFeed } from "./feed";

/** One externally enforced owner. Confirm -> durable checkpoint -> dispatched. */
export class Dispatcher extends EventEmitter<DispatcherEvents> {
  private readonly watcher: DurableCommitWatcher;
  private readonly publisher: CommitPublisher;
  private running = false;
  private stopped = false;
  private stopping?: Promise<void>;
  private readonly feed: string;

  // Node's emitter permits unknown strings; this SDK exposes declared events and Node's observer hooks.
  override on<E extends string | symbol>(event: E & (keyof DispatcherEvents | "newListener" | "removeListener" | symbol),
    listener: Parameters<typeof this.addListener<E>>[1]): this {
    return super.on(event, listener);
  }

  constructor(private readonly config: DispatcherConfig) {
    super();
    validateDispatcherConfig(config);
    this.feed = checkpointFeed(config);
    this.watcher = config.watchMode === "oplog" ? new OplogWatcher(config.mongodb) : new ChangeStreamWatcher(config.mongodb);
    this.publisher = new CommitPublisher(config.rabbitmq, config.tenant, config.publication);
    this.watcher.on("fallback", () => this.emit("fallback"));
    this.watcher.on("recovery", (event: import("./types").RecoveryEvent) => this.emit("recovery", event));
    this.watcher.on("error", (error: Error) => this.emit("error", error));
  }

  private async load(): Promise<ResumeState | null> {
    const value: unknown = await this.config.resumeTokenStore.load();
    if (value === null) return null;
    const state = decodeState(value);
    if (state.feed !== this.feed && !(state.feed === undefined && this.config.adoptLegacyCheckpoint)) {
      throw new Error("Checkpoint feed mismatch or unidentifiable legacy state; explicit migration required");
    }
    return state;
  }

  async start(): Promise<void> {
    if (this.running || this.stopped) throw new Error("Dispatcher already started/stopped; one owner required");
    this.running = true;
    try {
      const state = await this.load();
      await this.watcher.connect();
      await this.publisher.connect();
      if (this.isStopped()) return;
      if (state) this.emit("resumed", state);
      this.emit("started");
      await this.watcher.startWithProgress(state, (commit, progress) => this.handle(commit, progress));
    } catch (cause: unknown) {
      const error = cause instanceof Error ? cause : new Error(String(cause));
      await this.stop();
      this.emit("fatal", error);
      throw error;
    } finally { await this.stop(); this.running = false; }
  }

  private async handle(commit: ICommit | undefined, progress: DurableProgress): Promise<void> {
    const result = await deliverOutcome(commit, progress, { publisher: this.publisher, store: this.config.resumeTokenStore,
      collection: this.config.mongodb.collection, feed: this.feed, failureHandler: this.config.failureHandler });
    if (result.kind === "dispatched" && commit) this.emit("dispatched", commit);
  }

  private isStopped(): boolean { return this.stopped; }

  stop(): Promise<void> {
    if (this.stopping) return this.stopping;
    this.stopped = true;
    this.stopping = this.cleanup();
    return this.stopping;
  }

  private async cleanup(): Promise<void> {
    try { await this.watcher.stop(); }
    finally { await this.publisher.close(); this.emit("stopped"); }
  }
}
