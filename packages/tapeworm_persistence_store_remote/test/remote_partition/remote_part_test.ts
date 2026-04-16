import { describe, it, expect, beforeEach } from "vitest";
import Tapeworm, { Commit } from "tapeworm";
import SocketMock from "socket.io-mock";
import TapewormSyncServer from "../../src/server";
import { RemotePersistence, RemoteClient } from "../../src/client";

var autobus = {
  join: function () {
    return null;
  },
  leave: function () {},
};

describe("Remote partition server/client", function () {
  var socketServer: SocketMock;
  var socketClient: SocketMock;
  var clientPartition: Record<string, Function>;
  var serverPartition: Record<string, Function>;

  beforeEach(async function () {
    socketServer = new SocketMock();
    socketClient = socketServer.socketClient;
    var rrtwClient = new RemoteClient(
      socketClient as ConstructorParameters<typeof RemoteClient>[0],
      "/tw",
    );
    var remotePartition = new RemotePersistence(rrtwClient, "blondie");

    var clientTapeworm = new Tapeworm(
      remotePartition as ConstructorParameters<typeof Tapeworm>[0],
    );
    var tapeworm = new Tapeworm();
    var blondie = await tapeworm.openPartition("blondie");
    serverPartition = blondie as unknown as Record<string, Function>;
    var blondie2 = await clientTapeworm.openPartition("blondie");
    clientPartition = blondie2 as unknown as Record<string, Function>;
  });

  it("#socketMock should work", function () {
    return new Promise<void>(function (resolve) {
      socketClient.on("ping", function (...args: unknown[]) {
        var message = args[0] as string;
        expect(message).toBe("Hello");
        socketClient.emit("pong", "heya");
      });
      socketServer.on("pong", function (...args: unknown[]) {
        var message = args[0] as string;
        expect(message).toBe("heya");
        resolve();
      });
      socketServer.emit("ping", "Hello");
    });
  });

  it("#loadSnapshot should work on remote partition", function () {
    return new Promise<void>(function (resolve) {
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.storeSnapshot("1", { test: "success" }, 2, function () {
        clientPartition.loadSnapshot(
          "1",
          function (_err: Error | null, res: Record<string, unknown>) {
            expect((res.snapshot as Record<string, unknown>).test).toBe(
              "success",
            );
            resolve();
          },
        );
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#loadSnapshot should work on remote partition with promise", function () {
    return new Promise<void>(function (resolve) {
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.storeSnapshot("1", { test: "success" }, 2, function () {
        clientPartition.loadSnapshot("1").then(function (
          res: Record<string, unknown>,
        ) {
          expect((res.snapshot as Record<string, unknown>).test).toBe(
            "success",
          );
          resolve();
        });
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#loadSnapshot should include events not persisted in snapshot", function () {
    return new Promise<void>(function (resolve) {
      var commits = [
        new Commit("1", "blondie", "1", 0, [{ id: "1", type: "registered" }]),
        new Commit("2", "blondie", "1", 1, [
          { id: "1", type: "amended1" },
          { id: "1", type: "amended2" },
        ]),
        new Commit("3", "blondie", "1", 2, [
          { id: "1", type: "amended3" },
          { id: "1", type: "amended4" },
        ]),
      ];
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.storeSnapshot("1", { test: "success" }, 2, function () {
        serverPartition.append(commits).then(function () {
          clientPartition.loadSnapshot("1", true).then(function (
            res: Record<string, unknown>,
          ) {
            var snapshot = res.snapshot as Record<string, unknown>;
            expect((snapshot.snapshot as Record<string, unknown>).test).toBe(
              "success",
            );
            var resCommits = res.commits as Array<Record<string, unknown>>;
            expect((resCommits[0] as Record<string, unknown>).streamId).toBe(
              "1",
            );
            expect(
              ((resCommits[0] as Record<string, unknown>).events as unknown[])
                .length,
            ).toBe(2);
            resolve();
          });
        });
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#queryStreamWithSnapshot should include events not persisted in snapshot", function () {
    return new Promise<void>(function (resolve) {
      var commits = [
        new Commit("1", "blondie", "1", 0, [{ id: "1", type: "registered" }]),
        new Commit("2", "blondie", "1", 1, [
          { id: "1", type: "amended1" },
          { id: "1", type: "amended2" },
        ]),
        new Commit("3", "blondie", "1", 2, [
          { id: "1", type: "amended3" },
          { id: "1", type: "amended4" },
        ]),
      ];
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.storeSnapshot("1", { test: "success" }, 2, function () {
        serverPartition.append(commits).then(function () {
          clientPartition.queryStreamWithSnapshot("1").then(function (
            res: Record<string, unknown>,
          ) {
            var snapshot = res.snapshot as Record<string, unknown>;
            expect((snapshot.snapshot as Record<string, unknown>).test).toBe(
              "success",
            );
            var resCommits = res.commits as Array<Record<string, unknown>>;
            expect((resCommits[0] as Record<string, unknown>).streamId).toBe(
              "1",
            );
            expect(
              ((resCommits[0] as Record<string, unknown>).events as unknown[])
                .length,
            ).toBe(2);
            resolve();
          });
        });
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#queryStream should work on remote partition", function () {
    return new Promise<void>(function (resolve) {
      var commits = [
        new Commit("1", "blondie", "1", 0, [{ id: "1", type: "registered" }]),
        new Commit("2", "blondie", "1", 1, [{ id: "1", type: "amended" }]),
      ];
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.append(commits).then(function () {
        clientPartition.queryStream(
          "1",
          function (_err: Error | null, res: Array<Record<string, unknown>>) {
            expect(res.length).toBe(2);
            expect(
              (res[0].events as Array<Record<string, unknown>>)[0].type,
            ).toBe("registered");
            expect(
              (res[1].events as Array<Record<string, unknown>>)[0].type,
            ).toBe("amended");
            resolve();
          },
        );
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#queryStream should work on remote partition with promise", function () {
    return new Promise<void>(function (resolve) {
      var commits = [
        new Commit("1", "blondie", "1", 0, [{ id: "1", type: "registered" }]),
        new Commit("2", "blondie", "1", 1, [{ id: "1", type: "amended" }]),
      ];
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.append(commits).then(function () {
        clientPartition.queryStream("1").then(function (
          res: Array<Record<string, unknown>>,
        ) {
          expect(res.length).toBe(2);
          expect(
            (res[0].events as Array<Record<string, unknown>>)[0].type,
          ).toBe("registered");
          expect(
            (res[1].events as Array<Record<string, unknown>>)[0].type,
          ).toBe("amended");
          resolve();
        });
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
  it("#queryStream should work on remote partition from commit sequence", function () {
    return new Promise<void>(function (resolve) {
      var commits = [
        new Commit("1", "blondie", "1", 0, [{ id: "1", type: "registered" }]),
        new Commit("2", "blondie", "1", 1, [{ id: "1", type: "amended" }]),
      ];
      var tapewormSyncServer = new TapewormSyncServer(
        serverPartition as ConstructorParameters<typeof TapewormSyncServer>[0],
        autobus,
      );
      serverPartition.append(commits).then(function () {
        clientPartition.queryStream(
          "1",
          0,
          function (_err: Error | null, res: Array<Record<string, unknown>>) {
            expect(res.length).toBe(1);
            expect(
              (res[0].events as Array<Record<string, unknown>>)[0].type,
            ).toBe("amended");
            resolve();
          },
        );
      });
      tapewormSyncServer.addSocket(
        socketServer as ConstructorParameters<
          typeof TapewormSyncServer.prototype.addSocket
        >[0],
      );
    });
  });
});
