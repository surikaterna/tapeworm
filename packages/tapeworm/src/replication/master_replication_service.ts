interface IMasterReplicationService {
  handle(_first_argument: unknown): void;
}

/** Abstract template — subclasses must override handle. */
var MasterReplicatonService = function () {} as unknown as new () => IMasterReplicationService;

MasterReplicatonService.prototype.handle = function (_first_argument: unknown) {
  // body...
};

export default MasterReplicatonService;
