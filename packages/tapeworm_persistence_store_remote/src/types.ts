/**
 * Minimal socket interface — duck-typed for both socket.io server and client sockets.
 * The remote store does not depend on socket.io directly; consumers provide the socket.
 */
export interface ISocket {
  id: string;
  on(event: string, listener: (...args: unknown[]) => void): void;
  emit(event: string, ...args: unknown[]): void;
  removeListener(event: string, listener: (...args: unknown[]) => void): void;
}

/**
 * Request/response protocol between client and server.
 */
export interface ITwRequest {
  i: number;
  p: Record<string, unknown>;
}

export interface ITwResponse {
  i: number;
  p: unknown;
  e?: string;
}

/**
 * Autobus interface for the server-side pub/sub.
 */
export interface IAutobus {
  join(channel: string, callback: (data: unknown) => void): unknown;
  leave(channel: string, handle: unknown): void;
}
