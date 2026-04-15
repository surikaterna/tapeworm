var _ = require('lodash');
var SubscriptionServer = function (tapeworm, autobus) {
  var self = this;
  this._sockets = {};
  this._tapeworm = tapeworm;
  this._autobus = autobus;
};

SubscriptionServer.prototype.addSocket = function (socket) {
  if (!this._sockets[socket.id]) {
    this._sockets[socket.id] = {
      socket: socket,
      streams: {}
    };
    this._initializeSocket(socket);
  } else {
    console.log("Socket already added");
  }
};

SubscriptionServer.prototype._initializeSocket = function (socket) {
  var self = this;
  socket.on('/tw/request', function (request) {
    if (request.p) {
      if (request.p.subscribe) {
        self._subscribe(socket, request);
      } else if (request.p.unsubscribe) {
        self._unsubscribe(socket, request);
      } else if (request.p.loadSnapshot) {
        var streamId = _.get(request, 'p.loadSnapshot.streamId');
        var includeSubsequentCommits = _.get(request, 'p.loadSnapshot.includeSubsequentCommits');
        self._loadSnapshot(request, socket, streamId, includeSubsequentCommits);
      } else if (request.p.queryStreamWithSnapshot) {
        var strmId = _.get(request, 'p.queryStreamWithSnapshot.streamId');
        self._loadSnapshot(request, socket, strmId, true);
      } else if (request.p.queryCommits) {
        self._tapeworm._queryStream(request.p.queryCommits.streamId).then(function (commits) {
          var cmts = _.filter(commits, function (commit) {
            return commit.commitSequence > request.p.queryCommits.fromSequence;
          });
          _sendCommits(socket, cmts, request.i);
        });
      } else {
        throw new Error("Unknown request from client: " + _.keys(request) + " || " + JSON.stringify(request.p));
      }
    }
  });
  socket.on('disconnect', function () {
    console.log("TW Socket disconnecting");
    self._destroySocket(socket);
  });
};

SubscriptionServer.prototype._subscribe = function (socket, request) {
  var socketDetails = this._sockets[socket.id];
  if (!socketDetails.streams[request.p.subscribe.streamId]) {
    var callback = function (commit) {
      console.log("Got commit");
      var c = _.clone(commit);
      c.authorative = true;
      _sendCommits(socket, commit, request.i);
    };

    var abHandle = this._autobus.join("/domain/" + request.p.subscribe.streamId + "/commit", callback);
    socketDetails.streams[request.p.subscribe.streamId] = {
      i: request.i,
      handle: abHandle
    };

  } else {
    console.log("Already subscribed...");
  }
};

SubscriptionServer.prototype._unsubscribe = function (socket, request) {
  var socketDetails = this._sockets[socket.id];
  if (socketDetails.streams[request.p.unsubscribe.streamId]) {
    var handle = socketDetails.streams[request.p.unsubscribe.streamId].handle;
    console.log("Leaving:" + handle);
    this._autobus.leave("/domain/" + request.p.unsubscribe.streamId + "/commit", handle);
    delete socketDetails.streams[request.p.unsubscribe.streamId];
  } else {
    console.log("Not subscribed...");
  }
};

SubscriptionServer.prototype._destroySocket = function (socket) {
  var self = this;
  var socketDetails = this._sockets[socket.id];
  if (socketDetails) {
    console.log("Unsubscribeing from all streams");
    _.forEach(socketDetails.streams, function (stream, n) {
      console.log("unsub: " + n);
      self._autobus.leave("/domain/" + n + "/commit", stream.cb);
    });
    delete this._sockets[socket.id];
  } else {
    console.log("Didnt find socket details ")
  }
};

SubscriptionServer.prototype._loadSnapshot = function (request, socket, streamId, includeSubsequentCommits) {
  var self = this;
  if (streamId) {
    self._tapeworm.loadSnapshot(streamId).then(function (snapshot) {
      if (includeSubsequentCommits) {
        self._tapeworm.queryStreamWithSnapshot(streamId).then(function (result) {
          socket.emit("/tw/response", { i: request.i, p: result });
        });
      } else {
        socket.emit("/tw/response", { i: request.i, p: snapshot });
      }
    })
  }
};

function _sendCommits(socket, commits, i) {
  if (!_.isArray(commits)) {
    commits = [commits];
  }
  var clonedCommits = _.cloneDeep(commits);
  _.forEach(clonedCommits, function (commit) {
    commit.authorative = true;
    _.forEach(commit.events, function (event) {
      event.authorative = true;
    });
  });
  socket.emit("/tw/response", {
    i: i,
    p: {
      commits: clonedCommits
    }
  });
}

module.exports = SubscriptionServer;
