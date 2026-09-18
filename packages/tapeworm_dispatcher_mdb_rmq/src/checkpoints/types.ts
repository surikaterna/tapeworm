import type { ResumeState } from "../types";

/** Durable roundtrip of every field, preserving BSON Timestamp/token values.
 * Legacy three-field implementations must be upgraded before recovery is enabled.
 */
export interface IResumeTokenStore {
  load(): Promise<ResumeState | null>;
  save(state: ResumeState): Promise<void>;
}
