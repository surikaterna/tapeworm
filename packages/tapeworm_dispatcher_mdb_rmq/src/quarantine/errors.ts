import type { QuarantinedEvent } from "./types";

export class QuarantinePaused extends Error {
  constructor(readonly event: QuarantinedEvent) { super("Quarantine paused; operator redrive and restart required"); }
}
