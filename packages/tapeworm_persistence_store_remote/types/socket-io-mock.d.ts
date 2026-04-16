declare module "socket.io-mock" {
  import { EventEmitter } from "events";

  class SocketMock extends EventEmitter {
    id: string;
    socketClient: SocketMock;
    emit(event: string, ...args: unknown[]): boolean;
    on(event: string, listener: (...args: unknown[]) => void): this;
    removeListener(event: string, listener: (...args: unknown[]) => void): this;
  }

  export = SocketMock;
}
