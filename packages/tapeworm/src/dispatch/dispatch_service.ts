import type { ICommit, NodeCallback } from "../types";

interface IDispatchService {
  dispatch(_commit: ICommit, _callback?: NodeCallback<void>): void;
}

/** Abstract template — subclasses must override dispatch. */
var DispatchService = function () {} as unknown as new () => IDispatchService;

DispatchService.prototype.dispatch = function (_commit: ICommit, _callback?: NodeCallback<void>) {
  // body...
};

export default DispatchService;
