import type { ResumeState } from "../types";

/** Persistence interface for dispatcher resume tokens. */
export interface IResumeTokenStore {
  load(): Promise<ResumeState | null>;
  save(state: ResumeState): Promise<void>;
}
