import { EventEmitter } from "node:events";
import type { ICommit } from "tapeworm";
import type { DurableProgress, MongoConfig, PrimaryPosition, RecoveryEvent, ResumeState } from "../types";
import type { HistoryPort } from "./history";
import type { LiveItem, LivePort } from "./live-source";
import { HistoryExpired, RecoveryExhausted } from "./recovery-errors";
import { decodeState, timestamp, uuid } from "../validation";
import { DeliveryHalted } from "../delivery/delivery-halted";

export type CommitHandler = (commit: ICommit, resumeToken: Record<string, unknown>) => Promise<void>;
export type ProgressHandler = (commit: ICommit | undefined, progress: DurableProgress) => Promise<void>;
export interface WatcherEvents {
  error: [error: Error]; fatal: [error: Error]; fallback: []; recovery: [event: RecoveryEvent];
}
export interface ICommitWatcher extends EventEmitter<WatcherEvents> {
  connect(): Promise<void>;
  start(state: ResumeState | null, handler: CommitHandler): Promise<void>;
  stop(): Promise<void>;
}
export interface DurableCommitWatcher extends ICommitWatcher {
  startWithProgress(state: ResumeState | null, handler: ProgressHandler): Promise<void>;
}

export class RecoveryWatcher extends EventEmitter<WatcherEvents> implements DurableCommitWatcher {
  private running = false;
  private stopped = true;
  private connected = false;
  private state: ResumeState = { updatedAt: new Date() };
  private wake?: () => void;
  private pendingTransition?: ResumeState;

  constructor(private readonly live: LivePort, private readonly history: HistoryPort,
    private readonly config: Pick<MongoConfig, "maxRetries" | "retryDelayMs">) {
    super();
    if (!Number.isSafeInteger(config.maxRetries ?? 50) || (config.maxRetries ?? 50) < 1) {
      throw new Error("maxRetries must be a positive integer");
    }
    if (!Number.isSafeInteger(config.retryDelayMs ?? 1000) || (config.retryDelayMs ?? 1000) < 0) {
      throw new Error("retryDelayMs must be a nonnegative integer");
    }
  }

  connect(): Promise<void> { this.connected = true; return Promise.resolve(); }

  async start(state: ResumeState | null, handler: CommitHandler): Promise<void> {
    await this.startWithProgress(state, async (commit, progress) => {
      if (progress.kind === "replay" || progress.state.recovery) {
        throw new RecoveryExhausted("Legacy handler cannot persist recovery; use startWithProgress");
      }
      if (!commit || progress.kind !== "live") return;
      const position = progress.position;
      if (position.kind === "boundary") throw new Error("Invalid commit boundary");
      await handler(commit, position.kind === "changeStream" ? position.token : { ts: position.ts });
    });
  }

  async startWithProgress(state: ResumeState | null, handler: ProgressHandler): Promise<void> {
    if (this.running) throw new Error("Watcher already running; one owner required");
    if (!this.connected) throw new Error("connect() must be called before start()");
    this.state = state ? decodeState(state) : { updatedAt: new Date() };
    this.pendingTransition = undefined;
    this.running = true;
    this.stopped = false;
    try { await this.run(handler); }
    finally { await this.closeCursors(); this.running = false; }
  }

  private async run(handler: ProgressHandler): Promise<void> {
    let failures = 0;
    while (!this.isStopped()) {
      try { await this.attempt(handler); }
      catch (cause: unknown) {
        if (this.isStopped()) return;
        const error = cause instanceof Error ? cause : new Error(String(cause));
        if (error instanceof RecoveryExhausted || error instanceof DeliveryHalted || ++failures >= (this.config.maxRetries ?? 50)) {
          this.emit("fatal", error);
          throw error;
        }
        this.emit("error", error);
      } finally { await this.closeCursors(); }
      if (!this.isStopped()) await this.sleep();
    }
  }

  private position(): PrimaryPosition | undefined {
    if (this.state.primary) return this.state.primary;
    const token = this.state.changeStreamToken;
    if (!token) return undefined;
    return this.live.mode === "oplog" ? { kind: "oplog", ts: timestamp(token.ts) }
      : { kind: "changeStream", token };
  }

  private async attempt(handler: ProgressHandler): Promise<void> {
    if (this.pendingTransition) await this.accept(undefined, this.pendingTransition, "transition", handler);
    if (this.state.recovery?.phase === "scan") await this.replay(handler);
    let position = this.position();
    if (!position && this.state.lastCommitToken) {
      await this.beginRecovery("legacy-uuid", handler);
      await this.replay(handler);
      position = this.position();
    }
    if (!position) {
      position = { kind: "boundary", mode: this.live.mode, ts: await this.history.boundary() };
      await this.accept(undefined, { ...this.state, primary: position }, "transition", handler);
    }
    await this.consume(position, handler);
  }

  private async consume(position: PrimaryPosition, handler: ProgressHandler): Promise<void> {
    const iterator = this.live.watch(position)[Symbol.asyncIterator]();
    try {
      while (!this.isStopped()) {
        const next = await this.next(iterator, handler);
        if (!next) return;
        if (next.done) throw new Error("Live cursor ended unexpectedly");
        if (this.isStopped()) return;
        const { commit, position: primary } = next.value;
        const recovered = this.state.recovery !== undefined;
        const token = primary.kind === "changeStream" ? primary.token : { ts: primary.ts };
        await this.accept(commit, { ...this.state, primary, changeStreamToken: token,
          recovery: undefined, lastCommitToken: uuid(commit.token) }, "live", handler);
        if (recovered) this.emit("recovery", { phase: "live", state: this.state });
      }
    } finally { await iterator.return?.(); }
  }

  private async next(iterator: AsyncIterator<LiveItem, void, unknown>, handler: ProgressHandler)
    : Promise<IteratorResult<LiveItem, void> | undefined> {
    try { return await iterator.next(); }
    catch (error: unknown) {
      if (!(error instanceof HistoryExpired)) throw error;
      if (this.state.recovery) throw new RecoveryExhausted("Live boundary expired during recovery; operator action required");
      await this.beginRecovery("history-expired", handler);
      return undefined;
    }
  }

  private async beginRecovery(reason: "history-expired" | "legacy-uuid", handler: ProgressHandler): Promise<void> {
    if (!this.state.lastCommitToken) throw new RecoveryExhausted("Expired history without UUID checkpoint; operator action required");
    const boundary = await this.history.boundary();
    const upper = await this.history.upper();
    const recovery = { phase: "scan" as const, boundary, upper,
      lower: this.state.lastCommitToken, startedAt: new Date() };
    await this.accept(undefined, { ...this.state, recovery }, "transition", handler);
    if (reason === "history-expired") this.emit("fallback");
    this.emit("recovery", { phase: "started", reason, state: this.state });
  }

  private async replay(handler: ProgressHandler): Promise<void> {
    const recovery = this.state.recovery;
    if (!recovery) throw new Error("Missing recovery state");
    for await (const commit of this.history.scan(recovery)) {
      if (this.stopped) return;
      await this.accept(commit, { ...this.state, lastCommitToken: uuid(commit.token),
        recovery: { ...recovery, cursor: uuid(commit.token) } }, "replay", handler);
      this.emit("recovery", { phase: "scan", state: this.state });
    }
    if (this.stopped) return;
    await this.accept(undefined, { ...this.state, changeStreamToken: undefined,
      primary: { kind: "boundary", mode: this.live.mode, ts: recovery.boundary },
      recovery: { ...recovery, cursor: this.state.recovery?.cursor, phase: "cutover" } }, "transition", handler);
    this.emit("recovery", { phase: "cutover", state: this.state });
  }

  private async accept(commit: ICommit | undefined, state: ResumeState,
    kind: DurableProgress["kind"], handler: ProgressHandler): Promise<void> {
    const next = { ...state, version: 1 as const, updatedAt: new Date() };
    const progress = this.progress(kind, next);
    if (kind === "transition") this.pendingTransition = next;
    await handler(commit, progress);
    this.state = next;
    this.pendingTransition = undefined;
  }

  private progress(kind: DurableProgress["kind"], state: ResumeState): DurableProgress {
    if (kind !== "live") return { kind, state };
    if (!state.primary) throw new Error("Missing live primary position");
    return { kind, state, position: state.primary };
  }

  private sleep(): Promise<void> {
    return new Promise((resolve) => {
      const done = () => { clearTimeout(timer); this.wake = undefined; resolve(); };
      const timer = setTimeout(done, this.config.retryDelayMs ?? 1000);
      this.wake = done;
    });
  }

  private isStopped(): boolean { return this.stopped; }

  private async closeCursors(): Promise<void> { await Promise.all([this.live.close(), this.history.close()]); }
  async stop(): Promise<void> { this.stopped = true; this.wake?.(); await this.closeCursors(); }
}
