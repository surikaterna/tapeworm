import _ from "lodash";
import Handle from "./handle";
import type { ICommit } from "tapeworm";

type ClientInstance = InstanceType<typeof import("./rrtw_client").default>;
type HandleInstance = InstanceType<typeof Handle>;

interface ISubscription {
  count: number;
  handle: HandleInstance;
  _handle?: { stop: () => void };
}

export interface IEventStreamSubscriber {
  _subscriptions: Record<string, ISubscription>;
  _versionProvider: (streamId: string) => Promise<number>;
  _client: ClientInstance;
  _callback: (
    commits: ICommit[],
    callback: (err: Error | null, commits?: ICommit[]) => void,
  ) => void;
  subscribe(streamId: string): HandleInstance;
  activeStreams(): string[];
  _createSubscription(sub: ISubscription, streamId: string): void;
  _destroySubscription(sub: ISubscription, streamId: string): void;
  _publish(
    commits: ICommit[],
    callback: (err: Error | null, commits?: ICommit[]) => void,
  ): void;
}

var EventStreamSubscriber = function (
  this: IEventStreamSubscriber,
  client: ClientInstance,
  versionProvider: (streamId: string) => Promise<number>,
  commitsCallback: (
    commits: ICommit[],
    callback: (err: Error | null, commits?: ICommit[]) => void,
  ) => void,
) {
  this._subscriptions = {};
  this._versionProvider = versionProvider;
  this._client = client;
  this._callback = commitsCallback;
} as unknown as new (
  client: ClientInstance,
  versionProvider: (streamId: string) => Promise<number>,
  commitsCallback: (
    commits: ICommit[],
    callback: (err: Error | null, commits?: ICommit[]) => void,
  ) => void,
) => IEventStreamSubscriber;

EventStreamSubscriber.prototype.subscribe = function (
  this: IEventStreamSubscriber,
  streamId: string,
): HandleInstance {
  var self = this;
  var sub = this._subscriptions[streamId];
  if (!sub) {
    var stop = function () {
      if (!sub.count) {
        throw new Error("Subscription already destroyed");
      } else if (--sub.count === 0) {
        self._destroySubscription(sub, streamId);
        delete self._subscriptions[streamId];
      }
    };
    sub = {
      count: 0,
      handle: new Handle(stop),
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

EventStreamSubscriber.prototype.activeStreams = function (
  this: IEventStreamSubscriber,
): string[] {
  return _.keys(this._subscriptions);
};

EventStreamSubscriber.prototype._createSubscription = function (
  this: IEventStreamSubscriber,
  sub: ISubscription,
  streamId: string,
) {
  var self = this;
  this._versionProvider(streamId).then(function (currentSequence: number) {
    self._client.request(
      { queryCommits: { streamId: streamId, fromSequence: currentSequence } },
      function (_err: Error | null, pkg: unknown) {
        var p = pkg as { commits: ICommit[] };
        // send 'insync' event, but wait until commits have been committed to local tw
        self._publish(
          p.commits,
          function (err: Error | null, commits?: ICommit[]) {
            if (err) {
              sub.handle.emit("error", err);
            } else {
              sub.handle.emit("insync", { commits: commits });
            }
            // start this only after the first 'insync' event has been receievd
            sub._handle = self._client.subscribe(
              { subscribe: { streamId: streamId } },
              function (_err: Error | null, pkg2: unknown) {
                var p2 = pkg2 as { commits: ICommit[] };
                self._publish(
                  p2.commits,
                  function (err: Error | null, commits?: ICommit[]) {
                    if (err) {
                      sub.handle.emit("error", err);
                    } else {
                      sub.handle.emit("commit", { commits: commits });
                    }
                  },
                );
              },
            );
          },
        );
        if (sub.count === 0) {
          // already unsubscribed
          self._destroySubscription(sub, streamId);
          // sub._handle.stop();
        }
      },
    );
  });
};

EventStreamSubscriber.prototype._destroySubscription = function (
  this: IEventStreamSubscriber,
  sub: ISubscription,
  streamId: string,
) {
  if (sub._handle) {
    this._client.request({ unsubscribe: { streamId: streamId } });
    sub._handle.stop();
  }
};

EventStreamSubscriber.prototype._publish = function (
  this: IEventStreamSubscriber,
  commits: ICommit[],
  callback: (err: Error | null, commits?: ICommit[]) => void,
) {
  this._callback(commits, callback);
};

export default EventStreamSubscriber;
