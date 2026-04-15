var _ = require('lodash');
var Handle = require('./handle');

var EventStreamSubscriber = function (client, versionProvider, commitsCallback) {
  this._subscriptions = {};
  this._versionProvider = versionProvider;
  this._client = client;
  this._callback = commitsCallback;
};

EventStreamSubscriber.prototype.subscribe = function (streamId) {
  var self = this;
  var sub = this._subscriptions[streamId];
  if (!sub) {
    var stop = function () {
      if (!sub.count) {
        throw new Error("Subscription already destroyed");
      }
      else if (--sub.count === 0) {
        self._destroySubscription(sub, streamId);
        delete self._subscriptions[streamId];
      }
    };
    sub = {
      count: 0,
      handle: new Handle(stop)
    };
    this._subscriptions[streamId] = sub;
  }
  sub.count++;
  if (sub.count === 1) {
    //first subscription for this stream
    self._createSubscription(sub, streamId);
  }
  return sub.handle;
};

EventStreamSubscriber.prototype.activeStreams = function () {
  return _.keys(this._subscriptions);
};

EventStreamSubscriber.prototype._createSubscription = function (sub, streamId) {
  var self = this;
  this._versionProvider(streamId).then(function (currentSequence) {
    self._client.request({ queryCommits: { streamId: streamId, fromSequence: currentSequence } }, function (err, pkg) {
      // send 'insync' event, but wait until commits have been committed to local tw
      self._publish(pkg.commits, function (err, commits) {
        if (err) {
          sub.handle.emit('error', err);
        } else {
          sub.handle.emit('insync', { commits: commits });
        }
        // start this only after the first 'insync' event has been receievd
        sub._handle = self._client.subscribe({ subscribe: { streamId: streamId } }, function (err, pkg) {
          self._publish(pkg.commits, function (err, commits) {
            if (err) {
              sub.handle.emit('error', err);
            } else {
              sub.handle.emit('commit', { commits: commits });
            }
          })
        });
      });
      if (sub.count === 0) {
        // already unsubscribed
        self._destroySubscription(sub, streamId);
        // sub._handle.stop();
      }
    });
  });
};

EventStreamSubscriber.prototype._destroySubscription = function (sub, streamId) {
  if (sub._handle) {
    this._client.request({ unsubscribe: { streamId: streamId } });
    sub._handle.stop();
  }
};

EventStreamSubscriber.prototype._publish = function (commits, callback) {
  this._callback(commits, callback);
};

module.exports = EventStreamSubscriber;
