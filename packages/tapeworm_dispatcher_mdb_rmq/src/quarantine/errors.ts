import type { QuarantinedEvent } from "./types";
import { DeliveryHalted } from "../delivery-halted";

export class QuarantinePaused extends DeliveryHalted {
  constructor(readonly event: QuarantinedEvent) { super("Quarantine explicitly paused; adjust size policy and restart, or perform explicit operator redrive and restart"); }
}
