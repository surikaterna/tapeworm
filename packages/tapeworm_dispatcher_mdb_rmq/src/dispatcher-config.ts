import type { DispatcherConfig } from "./types";

const options = {
  mongodb: true, rabbitmq: true, resumeTokenStore: true, tenant: true, watchMode: true,
  feedId: true, adoptLegacyCheckpoint: true, publication: true, failureHandler: true,
} satisfies Record<keyof DispatcherConfig, true>;

export function validateDispatcherConfig(config: unknown): void {
  if (!config || typeof config !== "object") throw new Error("Invalid dispatcher configuration");
  for (const key of Reflect.ownKeys(config)) {
    if (!Object.hasOwn(options, key)) {
      throw new Error(`Unknown dispatcher option: ${String(key)}; use failureHandler and adapter-owned events for delivery policies`);
    }
  }
  const handler = "failureHandler" in config ? config.failureHandler : undefined;
  if (handler !== undefined && (!handler || typeof handler !== "object" || !("handle" in handler) || typeof handler.handle !== "function")) {
    throw new Error("failureHandler must be an object with a handle function");
  }
}
