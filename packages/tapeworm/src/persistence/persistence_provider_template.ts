import type { NodeCallback } from "../types";

interface IPersistenceProviderTemplate {
  openPartition(_partitionId?: string, _callback?: NodeCallback<unknown>): void;
  deletePartition(_partitionId?: string, _callback?: NodeCallback<void>): void;
  close(_callback?: NodeCallback<void>): void;
}

/** Abstract template — subclasses must override all methods. */
var PersistenceProvider = function () {} as unknown as new () => IPersistenceProviderTemplate;

PersistenceProvider.prototype.openPartition = function (_partitionId?: string, _callback?: NodeCallback<unknown>) {
  // body...
};

PersistenceProvider.prototype.deletePartition = function (_partitionId?: string, _callback?: NodeCallback<void>) {};

PersistenceProvider.prototype.close = function (_callback?: NodeCallback<void>) {};

export default PersistenceProvider;
