import _ from "lodash";
import { Logger } from "slf";
import type { ISocket } from "../types";

var LOG = Logger.getLogger("tapeworm:rrtw-client");

interface IRequestInfo {
  cb: (err: Error | null, res: unknown) => void;
  k?: boolean;
  payload?: Record<string, unknown>;
}

export interface IClient {
  _prefix: string;
  _socket: ISocket;
  _requests: Record<number, IRequestInfo>;
  _requestId: number;
  _stopSubs: () => void;
  stop(): void;
  request(
    payload: Record<string, unknown>,
    callback?: (err: Error | null, res: unknown) => void,
    persistent?: boolean,
  ): number;
  subscribe(
    payload: Record<string, unknown>,
    callback: (err: Error | null, res: unknown) => void,
  ): { stop: () => void };
  _request(payload: Record<string, unknown>, id: number): number;
  _resubscribe(): void;
}

var Client = function (this: IClient, socket: ISocket, prefix?: string) {
  this._prefix = prefix || "/vdb";
  this._socket = socket;
  this._requests = {};
  this._requestId = 10;
  var self = this;

  var onReconnect = function () {
    self._resubscribe();
  };
  var onResponse = function (...args: unknown[]) {
    var event = args[0] as { i: number; p: unknown; e?: string };
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

  this._socket.on("reconnect", onReconnect);
  this._socket.on(this._prefix + "/response", onResponse);
  this._stopSubs = function () {
    self._socket.removeListener("reconnect", onReconnect);
    self._socket.removeListener(self._prefix + "/response", onResponse);
  };
} as unknown as new (socket: ISocket, prefix?: string) => IClient;

Client.prototype.stop = function (this: IClient) {
  this._stopSubs();
};

Client.prototype.request = function (
  this: IClient,
  payload: Record<string, unknown>,
  callback?: (err: Error | null, res: unknown) => void,
  persistent?: boolean,
): number {
  var requestInfo: IRequestInfo = {
    cb: callback!,
    k: persistent,
  };
  if (persistent) {
    requestInfo.payload = payload;
  }
  var id = this._requestId++;
  this._requests[id] = requestInfo;
  return this._request(payload, id);
};

Client.prototype.subscribe = function (
  this: IClient,
  payload: Record<string, unknown>,
  callback: (err: Error | null, res: unknown) => void,
): { stop: () => void } {
  var self = this;
  var i = this.request(payload, callback, true);
  return {
    stop: function () {
      delete self._requests[i];
    },
  };
};

Client.prototype._request = function (
  this: IClient,
  payload: Record<string, unknown>,
  id: number,
): number {
  var req = {
    i: id,
    p: payload,
  };
  this._socket.emit(this._prefix + "/request", req);
  return req.i;
};

Client.prototype._resubscribe = function (this: IClient) {
  var self = this;
  _.forEach(this._requests, function (req: IRequestInfo, i: string) {
    self._request(req.payload!, Number(i));
  });
};

export default Client;
