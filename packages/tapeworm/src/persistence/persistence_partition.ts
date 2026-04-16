import type { ICommit, NodeCallback } from "../types";

interface IPersistencePartitionTemplate {
  append(_commit: ICommit, _callback?: NodeCallback<ICommit>): void;
  getUndispatched(_callback?: NodeCallback<ICommit[]>): void;
  markAsDispatched(_commit: ICommit, _callback?: NodeCallback<void>): void;
  queryAll(_callback?: NodeCallback<ICommit[]>): void;
  queryStream(_streamId: string, _callback?: NodeCallback<ICommit[]>): void;
}

/** Abstract template — subclasses must override all methods. */
var PersistencePartition = function () {} as unknown as new () => IPersistencePartitionTemplate;

PersistencePartition.prototype.append = function (_commit: ICommit, _callback?: NodeCallback<ICommit>) {
  //add commit
};

PersistencePartition.prototype.getUndispatched = function (_callback?: NodeCallback<ICommit[]>) {
  //return events which are not dispatched
};

PersistencePartition.prototype.markAsDispatched = function (_commit: ICommit, _callback?: NodeCallback<void>) {
  //return events which are not dispatched
};

PersistencePartition.prototype.queryAll = function (_callback?: NodeCallback<ICommit[]>) {
  //return Query
};

PersistencePartition.prototype.queryStream = function (_streamId: string, _callback?: NodeCallback<ICommit[]>) {
  //return Query;
};

export default PersistencePartition;
