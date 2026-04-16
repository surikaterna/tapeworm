import { EventEmitter } from "events";
import EventStreamSubscriber from "./event_stream_sub";
import EventStreamSynchronizer from "./event_stream_sync";
import { getLastAuthorizedCommitSequence } from "./util";
import type { ICommit } from "tapeworm";
import type Handle from "./handle";

type ClientInstance = InstanceType<typeof import("./rrtw_client").default>;
type SynchronizerInstance = InstanceType<typeof EventStreamSynchronizer>;
type SubscriberInstance = InstanceType<typeof EventStreamSubscriber>;

/**
 * Minimal interface for the partition methods used by the agent.
 */
interface IAgentPartition {
  _queryStream(streamId: string): Promise<ICommit[]>;
  [key: string]: unknown;
}

class Agent extends EventEmitter {
  _client: ClientInstance;
  _partition: IAgentPartition;
  _synchronizer: SynchronizerInstance;
  _subscriber: SubscriberInstance;

  constructor(client: ClientInstance, partition: IAgentPartition) {
    super();
    this._client = client;
    this._partition = partition;
    this._synchronizer = new EventStreamSynchronizer(
      this._partition as unknown as ConstructorParameters<
        typeof EventStreamSynchronizer
      >[0],
    );
    this._subscriber = new EventStreamSubscriber(
      this._client,
      this._versionProvider.bind(this),
      this._commitsCallback.bind(this),
    );
  }

  start(): void {
    // :)
  }

  stop(): void {
    // :(
  }

  subscribe(streamId: string): Handle {
    var handle = this._subscriber.subscribe(streamId);
    return handle;
  }

  _commitsCallback(
    commits: ICommit[],
    callback: (err: Error | null, commits: ICommit[]) => void,
  ): void {
    var self = this;
    if (commits && commits.length > 0) {
      var commit = commits[0];
      var streamId = commit.streamId;
      var aggregateType = commit.aggregateType;
      this._synchronizer._processCommits(commits, streamId).then(function (
        conflicted: boolean,
      ) {
        if (conflicted) {
          self.emit("conflict", {
            streamId: streamId,
            aggregateType: aggregateType,
          });
        }
        callback(null, commits);
      });
    } else {
      callback(null, []);
    }
  }

  _versionProvider(streamId: string): Promise<number> {
    return this._partition._queryStream(streamId).then(function (
      localCommits: ICommit[],
    ) {
      var lastCommit = getLastAuthorizedCommitSequence(localCommits);
      return lastCommit;
    });
  }
}

export default Agent;
