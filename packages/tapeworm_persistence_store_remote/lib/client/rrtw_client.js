var _ = require('lodash');
var LOG = require('slf').Logger.getLogger('tapeworm:rrtw-client');

var Client = function (socket, prefix) {
  this._prefix = prefix || '/vdb';
  this._socket = socket;
  this._requests = {};
  this._requestId = 10;
  var self = this;

  var onReconnect = function () {
    self._resubscribe();
  };
  var onResponse = function (event) {
    if (event.e) {
      throw new Error(event.e);
    }
    var callback = self._requests[event.i];
    if (_.isUndefined(callback)) {
      LOG.debug("Response for unregistered request", event);
      throw new Error("Response for unregistered request: " + event.i);
    }
    if (!callback.k) {
      delete self._requests[event.i];
    }
    callback.cb(null, event.p);
  };

  this._socket.on('reconnect', onReconnect);
  this._socket.on(this._prefix + '/response', onResponse);
  this._stopSubs = function () {
    self._socket.removeListener('reconnect', onReconnect);
    self._socket.removeListener(self._prefix + '/response', onResponse);
  }
};

Client.prototype.stop = function () {
  this._stopSubs();
};

Client.prototype.request = function (payload, callback, persistent) {

  var requestInfo = {
    cb: callback, k: persistent
  };
  if (persistent) {
    requestInfo.payload = payload;
  }
  var id = this._requestId++;
  this._requests[id] = requestInfo;
  return this._request(payload, id);
};

Client.prototype.subscribe = function (payload, callback) {
  var self = this;
  var i = this.request(payload, callback, true);
  return {
    stop: function () {
      delete self._requests[i];
    }
  }
};

Client.prototype._request = function (payload, id) {
  var req = {
    i: id,
    p: payload,
  };
  this._socket.emit(this._prefix + '/request', req);
  return req.i;
};

Client.prototype._resubscribe = function () {
  var self = this;
  _.forEach(this._requests, function (req, i) {
    self._request(req.payload, i);
  });
};

module.exports = Client;
