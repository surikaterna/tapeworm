import _ from "lodash";
import type { ISocket, IAutobus } from "../types";
import type { ICommit } from "tapeworm";

/**
 * Minimal interface for the tapeworm partition methods used by the server.
 */
interface IServerPartition {
  _queryStream(streamId: string): Promise<ICommit[]>;
  loadSnapshot(streamId: string): Promise<unknown>;
  queryStreamWithSnapshot(streamId: string): Promise<unknown>;
  [key: string]: unknown;
}

interface ISocketDetails {
  socket: ISocket;
  streams: Record<
    string,
    { i: number; handle: unknown; cb?: (data: unknown) => void }
  >;
}

interface ITwRequest {
  i: number;
  p: Record<string, unknown>;
}

interface ISubscriptionServer {
  _sockets: Record<string, ISocketDetails>;
  _tapeworm: IServerPartition;
  _autobus: IAutobus;
  addSocket(socket: ISocket): void;
  _initializeSocket(socket: ISocket): void;
  _subscribe(socket: ISocket, request: ITwRequest): void;
  _unsubscribe(socket: ISocket, request: ITwRequest): void;
  _destroySocket(socket: ISocket): void;
  _loadSnapshot(
    request: ITwRequest,
    socket: ISocket,
    streamId: string,
    includeSubsequentCommits: unknown,
  ): void;
}

var SubscriptionServer = function (
  this: ISubscriptionServer,
  tapeworm: IServerPartition,
  autobus: IAutobus,
) {
  this._sockets = {};
  this._tapeworm = tapeworm;
  this._autobus = autobus;
} as unknown as new (
  tapeworm: IServerPartition,
  autobus: IAutobus,
) => ISubscriptionServer;

SubscriptionServer.prototype.addSocket = function (
  this: ISubscriptionServer,
  socket: ISocket,
) {
  if (!this._sockets[socket.id]) {
    this._sockets[socket.id] = {
      socket: socket,
      streams: {},
    };
    this._initializeSocket(socket);
  } else {
    console.log("Socket already added");
  }
};

SubscriptionServer.prototype._initializeSocket = function (
  this: ISubscriptionServer,
  socket: ISocket,
) {
  var self = this;
  socket.on("/tw/request", function (...args: unknown[]) {
    var request = args[0] as ITwRequest;
    if (request.p) {
      if (request.p.subscribe) {
        self._subscribe(socket, request);
      } else if (request.p.unsubscribe) {
        self._unsubscribe(socket, request);
      } else if (request.p.loadSnapshot) {
        var streamId = _.get(
          request,
          "p.loadSnapshot.streamId",
        ) as unknown as string;
        var includeSubsequentCommits = _.get(
          request,
          "p.loadSnapshot.includeSubsequentCommits",
        );
        self._loadSnapshot(request, socket, streamId, includeSubsequentCommits);
      } else if (request.p.queryStreamWithSnapshot) {
        var strmId = _.get(
          request,
          "p.queryStreamWithSnapshot.streamId",
        ) as unknown as string;
        self._loadSnapshot(request, socket, strmId, true);
      } else if (request.p.queryCommits) {
        var queryCommits = request.p.queryCommits as {
          streamId: string;
          fromSequence: number;
        };
        self._tapeworm._queryStream(queryCommits.streamId).then(function (
          commits: ICommit[],
        ) {
          var cmts = _.filter(commits, function (commit: ICommit) {
            return commit.commitSequence > queryCommits.fromSequence;
          });
          _sendCommits(socket, cmts, request.i);
        });
      } else {
        throw new Error(
          "Unknown request from client: " +
            _.keys(request) +
            " || " +
            JSON.stringify(request.p),
        );
      }
    }
  });
  socket.on("disconnect", function () {
    console.log("TW Socket disconnecting");
    self._destroySocket(socket);
  });
};

SubscriptionServer.prototype._subscribe = function (
  this: ISubscriptionServer,
  socket: ISocket,
  request: ITwRequest,
) {
  var socketDetails = this._sockets[socket.id];
  var subscribePayload = request.p.subscribe as { streamId: string };
  if (!socketDetails.streams[subscribePayload.streamId]) {
    var callback = function (commit: unknown) {
      console.log("Got commit");
      var c = _.clone(commit as ICommit);
      (c as ICommit & { authorative: boolean }).authorative = true;
      _sendCommits(socket, commit as ICommit, request.i);
    };

    var abHandle = this._autobus.join(
      "/domain/" + subscribePayload.streamId + "/commit",
      callback,
    );
    socketDetails.streams[subscribePayload.streamId] = {
      i: request.i,
      handle: abHandle,
    };
  } else {
    console.log("Already subscribed...");
  }
};

SubscriptionServer.prototype._unsubscribe = function (
  this: ISubscriptionServer,
  socket: ISocket,
  request: ITwRequest,
) {
  var socketDetails = this._sockets[socket.id];
  var unsubscribePayload = request.p.unsubscribe as { streamId: string };
  if (socketDetails.streams[unsubscribePayload.streamId]) {
    var handle = socketDetails.streams[unsubscribePayload.streamId].handle;
    console.log("Leaving:" + handle);
    this._autobus.leave(
      "/domain/" + unsubscribePayload.streamId + "/commit",
      handle,
    );
    delete socketDetails.streams[unsubscribePayload.streamId];
  } else {
    console.log("Not subscribed...");
  }
};

SubscriptionServer.prototype._destroySocket = function (
  this: ISubscriptionServer,
  socket: ISocket,
) {
  var self = this;
  var socketDetails = this._sockets[socket.id];
  if (socketDetails) {
    console.log("Unsubscribeing from all streams");
    _.forEach(
      socketDetails.streams,
      function (
        stream: { i: number; handle: unknown; cb?: (data: unknown) => void },
        n: string,
      ) {
        console.log("unsub: " + n);
        self._autobus.leave("/domain/" + n + "/commit", stream.cb);
      },
    );
    delete this._sockets[socket.id];
  } else {
    console.log("Didnt find socket details ");
  }
};

SubscriptionServer.prototype._loadSnapshot = function (
  this: ISubscriptionServer,
  request: ITwRequest,
  socket: ISocket,
  streamId: string,
  includeSubsequentCommits: unknown,
) {
  var self = this;
  if (streamId) {
    self._tapeworm.loadSnapshot(streamId).then(function (snapshot: unknown) {
      if (includeSubsequentCommits) {
        self._tapeworm.queryStreamWithSnapshot(streamId).then(function (
          result: unknown,
        ) {
          socket.emit("/tw/response", { i: request.i, p: result });
        });
      } else {
        socket.emit("/tw/response", { i: request.i, p: snapshot });
      }
    });
  }
};

function _sendCommits(
  socket: ISocket,
  commits: ICommit | ICommit[],
  i: number,
) {
  if (!_.isArray(commits)) {
    commits = [commits];
  }
  var clonedCommits = _.cloneDeep(commits);
  _.forEach(clonedCommits, function (commit: ICommit) {
    (commit as ICommit & { authorative: boolean }).authorative = true;
    _.forEach(commit.events, function (event) {
      (event as Record<string, unknown>).authorative = true;
    });
  });
  socket.emit("/tw/response", {
    i: i,
    p: {
      commits: clonedCommits,
    },
  });
}

export default SubscriptionServer;
