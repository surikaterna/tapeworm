import type { QuarantinedEvent } from "./types";

export class QuarantinePaused extends Error {
  constructor(readonly event: QuarantinedEvent) { super("Quarantine explicitly paused; adjust size policy and restart, or perform explicit operator redrive and restart"); }
}
