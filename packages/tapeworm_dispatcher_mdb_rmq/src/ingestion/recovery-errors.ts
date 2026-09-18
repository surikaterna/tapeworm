import { record } from "../validation";

export class HistoryExpired extends Error {}
export class RecoveryExhausted extends Error {}

export function isHistoryExpired(error: unknown): boolean {
  if (error instanceof HistoryExpired) return true;
  try {
    const doc = record(error);
    return doc.code === 286 || doc.codeName === "ChangeStreamHistoryLost";
  } catch { return false; }
}

/** Only cursor acquisition/iteration goes through this boundary, never handlers. */
export async function cursorNext<T>(next: () => Promise<T>): Promise<T> {
  try { return await next(); }
  catch (error: unknown) {
    if (isHistoryExpired(error)) throw new HistoryExpired("Mongo resume history expired");
    throw error;
  }
}
