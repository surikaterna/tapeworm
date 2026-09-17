export interface CliClient { close(): Promise<void> }
export interface CliDispatcher { start(): Promise<void>; stop(): Promise<void> }
type Signal = "SIGINT" | "SIGTERM";
export interface CliSignals {
  once(signal: Signal, listener: () => void): unknown;
  removeListener(signal: Signal, listener: () => void): unknown;
}
export interface CliLifecycleOptions {
  shutdownTimeoutMs?: number;
  onDeadline?: (error: CliShutdownTimeout) => unknown;
  onLateError?: (error: unknown) => unknown;
}

export class CliShutdownTimeout extends Error {
  constructor(readonly timeoutMs: number, readonly failures: readonly unknown[]) {
    super(`CLI shutdown exceeded ${timeoutMs}ms; cleanup attempted, operation outcome may be unknown`,
      { cause: failures[0] });
    this.name = "CliShutdownTimeout";
  }
}

/** Start and shutdown are observed independently; neither can hide the other's failure. */
export function runCli(client: CliClient, initialize: () => Promise<CliDispatcher>,
  signals: CliSignals = process, options: CliLifecycleOptions = {}): Promise<void> {
  return new Promise((resolve, reject) => {
    new CliLifetime(client, signals, options, resolve, reject).run(initialize);
  });
}

class CliLifetime {
  private dispatcher?: CliDispatcher;
  private operationSettled = false;
  private requested = false;
  private finished = false;
  private expiring = false;
  private stopStarted = false;
  private stopSettled = false;
  private closeStarted = false;
  private closeSettled = false;
  private timer?: ReturnType<typeof setTimeout>;
  private readonly failures: unknown[] = [];
  private readonly timeoutMs: number;
  private readonly interrupt = () => { this.signal("SIGINT", this.interrupt); };
  private readonly terminate = () => { this.signal("SIGTERM", this.terminate); };

  constructor(private readonly client: CliClient, private readonly signals: CliSignals,
    private readonly options: CliLifecycleOptions, private readonly resolve: () => void,
    private readonly reject: (error: unknown) => void) {
    this.timeoutMs = options.shutdownTimeoutMs ?? 10000;
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1) throw new Error("Invalid shutdownTimeoutMs");
  }

  run(initialize: () => Promise<CliDispatcher>): void {
    try {
      this.signals.once("SIGINT", this.interrupt);
      this.signals.once("SIGTERM", this.terminate);
    } catch (error: unknown) {
      this.failure(error, true);
      this.operationSettled = true;
      this.request();
      return;
    }
    void this.execute(initialize);
  }

  private async execute(initialize: () => Promise<CliDispatcher>): Promise<void> {
    try {
      this.dispatcher = await initialize();
      if (!this.requested) await this.dispatcher.start();
    } catch (error: unknown) { this.failure(error, true); }
    finally {
      this.operationSettled = true;
      this.request();
      this.stop();
      if (this.stopSettled) this.close();
      this.finish();
    }
  }

  private signal(signal: Signal, listener: () => void): void {
    // Retain our handler throughout drain: another signal must not trigger Node's default exit.
    try { this.signals.once(signal, listener); }
    catch (error: unknown) { this.failure(error, false); }
    this.request();
  }

  private request(): void {
    if (this.requested) return;
    this.requested = true;
    // One referenced timer, started only at the first shutdown/completion/failure.
    this.timer = setTimeout(() => { this.expire(); }, this.timeoutMs);
    if (this.dispatcher) this.stop(); else this.close();
  }

  private stop(): void {
    if (!this.dispatcher || this.stopStarted) return;
    this.stopStarted = true;
    void this.stopDispatcher(this.dispatcher);
  }

  private async stopDispatcher(dispatcher: CliDispatcher): Promise<void> {
    let failed = false;
    try { await dispatcher.stop(); }
    catch (error: unknown) { failed = true; this.failure(error, false); }
    finally {
      this.stopSettled = true;
      // Successful stop admits a grace period for in-flight checkpoint work.
      // A failed stop cannot gate client cleanup; a stalled drain is bounded by expire().
      if (failed || this.operationSettled) this.close();
      this.finish();
    }
  }

  private close(): void {
    if (this.closeStarted) return;
    this.closeStarted = true;
    void this.closeClient();
  }

  private async closeClient(): Promise<void> {
    try { await this.client.close(); }
    catch (error: unknown) { this.failure(error, false); }
    finally { this.closeSettled = true; this.finish(); }
  }

  private detach(): void {
    const bindings: [Signal, () => void][] = [["SIGINT", this.interrupt], ["SIGTERM", this.terminate]];
    for (const [signal, listener] of bindings) {
      try { this.signals.removeListener(signal, listener); }
      catch (error: unknown) { this.failure(error, false); }
    }
  }

  private finish(): void {
    if (this.finished || this.expiring || !this.operationSettled || !this.closeSettled) return;
    if (this.dispatcher && !this.stopSettled) return;
    clearTimeout(this.timer);
    this.detach();
    this.finished = true;
    const errors = [...new Set(this.failures)];
    if (errors.length === 0) { this.resolve(); return; }
    if (errors.length === 1) { this.reject(errors[0]); return; }
    const primary = errors[0];
    const message = primary instanceof Error ? primary.message : "Unknown failure";
    this.reject(new AggregateError(errors, `CLI failed: ${message}; additional cleanup failures`, { cause: primary }));
  }

  private expire(): void {
    if (this.finished) return;
    this.expiring = true;
    clearTimeout(this.timer);
    this.stop();
    this.close();
    this.detach();
    const error = new CliShutdownTimeout(this.timeoutMs, [...new Set(this.failures)]);
    this.finished = true;
    // Invoke cleanup before the host's last-resort deadline policy, never exit here.
    try {
      const result = this.options.onDeadline?.(error);
      void Promise.resolve(result).catch((late: unknown) => { this.late(late); });
    } catch (late: unknown) { this.late(late); }
    this.reject(error);
  }

  private failure(error: unknown, primary: boolean): void {
    if (this.finished) { this.late(error); return; }
    if (primary) this.failures.unshift(error); else this.failures.push(error);
  }

  private late(error: unknown): void {
    // A failing diagnostic sink must not create an unhandled rejection or recursive reports.
    try { void Promise.resolve(this.options.onLateError?.(error)).catch(() => {}); }
    catch { /* The original failure has already been observed. */ }
  }
}
