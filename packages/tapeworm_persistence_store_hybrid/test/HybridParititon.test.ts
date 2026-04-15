import { vi } from 'vitest';
import HybridPersistence, { Partition, Persistence } from './../src/HybridPersistence';
// @ts-ignore
require('fake-indexeddb/auto'); // Sets up indexDB in global scope
import EventStore, { Commit, EventStorePartition } from 'tapeworm';
// @ts-ignore
import IndexDBPersistence from 'tapeworm_persistence_store_indexeddb/lib/idb_persistence';
import { Commit as CommitType } from '../src/utils';
import { changedSnapshotStoredTime } from './helpers/testUtils';

let hybridPersistence: HybridPersistence;
const LOCAL_DB_MOCK = 'test_DB_local';
const REMOTE_DB_MOCK = 'test_DB_remote';
let mockLocalPartition: Partition;
let mockRemotePartition: Partition;

beforeEach(async () => {
  // Clear fake index DB to have a pristine state for each test
  const FDBFactory = require('fake-indexeddb/lib/FDBFactory');
  indexedDB = new FDBFactory();

  const mockedLocalPersistence: Persistence = new IndexDBPersistence(indexedDB, LOCAL_DB_MOCK);
  const mockedRemotePersistence: Persistence = new IndexDBPersistence(indexedDB, REMOTE_DB_MOCK);

  // Setup hybrid persistence but keep references to partitions for easy test setup
  const localPartition = await mockedLocalPersistence.openPartition('master');
  const remotePartition = await mockedRemotePersistence.openPartition('master');
  mockLocalPartition = localPartition;
  mockRemotePartition = remotePartition;
  hybridPersistence = new HybridPersistence(mockedLocalPersistence, mockedRemotePersistence, false, 5, { loggingEnabled: false });
});

describe('Partition', () => {
  describe('#append', () => {
    it('Should return commit if added', async () => {
      const commitId = '1234';
      const eventStore = new EventStore(hybridPersistence);

      const partition: Partition = await eventStore.openPartition('master');
      const commits = await partition.append(new Commit(commitId, 'master', '1', 0, []));
      expect(commits.length).toBe(1);
      expect(commits[0].id).toBe(commitId);
    });

    it('Should run dispatch service if available', async () => {
      const commitId = '1234';

      let dispatchedCommit: CommitType<Record<string, any>> | undefined;
      const dispatchService = (commit: CommitType<Record<string, any>>) => {
        dispatchedCommit = commit;
      };
      const eventStore = new EventStore(hybridPersistence, dispatchService);

      const partition: Partition = await eventStore.openPartition('master');
      const commits = await partition.append(new Commit(commitId, 'master', '1', 0, []));
      expect(commits.length).toBe(1);
      // Allow microtasks to flush so dispatch runs
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(dispatchedCommit).toBeDefined();
      expect(dispatchedCommit!.id).toBe(commitId);
    });

    it('Should return after all commits are persisted', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const commit1 = new Commit('1234', 'master', 0, []);
      const commit2 = new Commit('5678', 'master', 1, []);

      const partition: EventStorePartition = await eventStore.openPartition('master');
      const commits: CommitType<Record<string, any>>[] = await partition.append([commit1, commit2]);
      expect(commits.length).toBe(2);
      expect(commits[0].id).toBe(commit1.id);
      expect(commits[1].id).toBe(commit2.id);
    });
  });

  describe('#loadSnapshot', () => {
    it('Should fallback to remote partition if local does not contain snapshot', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = vi.fn(mockLocalPartition.loadSnapshot);
      mockLocalPartition.loadSnapshot = loadSnapshotMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockRemotePartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1);
      const snapshot: Record<string, any> = await partition.loadSnapshot(snapshotId);
      expect(loadSnapshotMock.mock.calls.length).toBe(1);
      expect(snapshot.version).toBe(1);
    });

    it('Should automatically store snapshot from remote to local', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = vi.fn(mockLocalPartition.loadSnapshot);
      mockLocalPartition.loadSnapshot = loadSnapshotMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockRemotePartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1);
      const snapshot: Record<string, any> = await partition.loadSnapshot(snapshotId);
      const localSnapshot: Record<string, any> = await mockLocalPartition.loadSnapshot(snapshotId);
      delete localSnapshot.storedDateTime;
      delete snapshot.storedDateTime;

      expect(localSnapshot).toEqual(snapshot);
    });

    it('Should use local partition if it contains snapshot and TTL has not expired', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = vi.fn(mockRemotePartition.loadSnapshot);
      mockRemotePartition.loadSnapshot = loadSnapshotMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1);
      const snapshot: Record<string, any> | undefined = await partition.loadSnapshot(snapshotId);
      expect(loadSnapshotMock.mock.calls.length).toBe(0);
      expect(snapshot?.version).toBe(1);
    });

    it('Should use remote partition if local snapshot has passed its TTL', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadRemoteSnapshotMock = vi.fn(mockRemotePartition.loadSnapshot);
      mockRemotePartition.loadSnapshot = loadRemoteSnapshotMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'id' }, 1);
      // Manipulate time stored to simulate an old snapshot
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      await changedSnapshotStoredTime(indexedDB, LOCAL_DB_MOCK, snapshotId, yesterday.toISOString());
      const snapshot: Record<string, any> | undefined = await partition.loadSnapshot(snapshotId);
      expect(loadRemoteSnapshotMock.mock.calls.length).toBe(1);
    });
  });

  describe('#queryStream', () => {
    it('Should fallback to remote partition if local does not have stream locally', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = vi.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockRemotePartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] });
      const queryStream: Record<string, any> = await partition.queryStream(snapshotId);
      expect(queryStreamMock.mock.calls.length).toBe(1);
      expect(queryStream.length).toBe(1);
    });

    it('Should use local partition if it has a snapshot that has not expired', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = vi.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'id' }, 1);
      await mockLocalPartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] });
      const queryStream = await partition.queryStream(snapshotId);
      expect(queryStream.length).toBe(1);
      expect(queryStreamMock.mock.calls.length).toBe(0);
    });

    it('Should use remote partition if snapshot has expired', async () => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = vi.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      const partition: Partition = await eventStore.openPartition('master');
      await mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'a' }, 1);
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      await changedSnapshotStoredTime(indexedDB, LOCAL_DB_MOCK, snapshotId, yesterday.toISOString());
      await mockRemotePartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] });
      const queryStream = await partition.queryStream(snapshotId);
      expect(queryStreamMock.mock.calls.length).toBe(1);
      expect(queryStream.length).toBe(1);
    });
  });
});
