import { createServer, connect, type Socket } from "node:net";
import { once } from "node:events";

/** Real TCP connection interruption without closing unrelated test connections. */
export async function rabbitProxy(uri: string) {
  const upstream = new URL(uri);
  const sockets = new Set<Socket>();
  let connections = 0;
  const remember = (socket: Socket) => {
    sockets.add(socket);
    socket.on("close", () => { sockets.delete(socket); });
    socket.on("error", () => { socket.destroy(); });
  };
  const server = createServer((socket) => {
    connections++;
    const backend = connect({ host: upstream.hostname, port: Number(upstream.port || 5672) });
    remember(socket); remember(backend);
    socket.pipe(backend).pipe(socket);
    socket.on("close", () => { backend.destroy(); });
    backend.on("close", () => { socket.destroy(); });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing proxy port");
  const downstream = new URL(uri);
  downstream.hostname = "127.0.0.1"; downstream.port = String(address.port);
  const disconnect = () => { for (const socket of sockets) socket.destroy(); };
  return { uri: downstream.toString(), disconnect, connections: () => connections, sockets: () => sockets.size,
    close: async () => {
      disconnect();
      const closed = once(server, "close"); server.close(); await closed;
    } };
}
