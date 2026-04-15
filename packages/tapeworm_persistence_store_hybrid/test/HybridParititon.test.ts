import HybridPersistence, { Partition, Persistence } from './../src/HybridPersistence';
// @ts-ignore
require('fake-indexeddb/auto'); // Sets up indexDB in global scope
// @ts-ignore
import EventStore, { Commit } from 'tapeworm';
// @ts-ignore
import IndexDBPersistence from 'tapeworm_persistence_store_indexeddb/lib/idb_persistence';
// @ts-ignore
import EventStorePartition from 'tapeworm/lib/event_store_partition';
import { Commit as CommitType } from '../src/utils';
import { changedSnapshotStoredTime } from './helpers/testUtils';

let hybridPersistence: HybridPersistence;
const LOCAL_DB_MOCK = 'test_DB_local';
const REMOTE_DB_MOCK = 'test_DB_remote';
let mockLocalPartition: Partition;
let mockRemotePartition: Partition;

beforeEach((done) => {
  // Clear fake index DB to have a pristine state for each test
  const FDBFactory = require('fake-indexeddb/lib/FDBFactory');
  indexedDB = new FDBFactory();

  const mockedLocalPersistence: Persistence = new IndexDBPersistence(indexedDB, LOCAL_DB_MOCK);
  const mockedRemotePersistence: Persistence = new IndexDBPersistence(indexedDB, REMOTE_DB_MOCK);

  // Setup hybrid persistence but keep references to partitions for easy test setup
  mockedLocalPersistence.openPartition('master').then((mockedLocalPartition) => {
    mockedRemotePersistence.openPartition('master').then((mockedRemotePartition) => {
      mockLocalPartition = mockedLocalPartition;
      mockRemotePartition = mockedRemotePartition;
      hybridPersistence = new HybridPersistence(mockedLocalPersistence, mockedRemotePersistence, false, 5, { loggingEnabled: false });
      done();
    });
  });
});

describe('Partition', () => {
  describe('#append', () => {
    it('Should return commit if added', (done) => {
      const commitId = '1234';
      const eventStore = new EventStore(hybridPersistence);

      eventStore.openPartition('master').then((partition: Partition) => {
        partition.append(new Commit(commitId, 'master', '1', 0, [])).then((commits) => {
          expect(commits.length).toBe(1);
          expect(commits[0].id).toBe(commitId);
          done();
        });
      });
    });

    it('Should run dispatch service if available', (done) => {
      const commitId = '1234';

      const dispatchService = (commit: CommitType<Record<string, any>>) => {
        expect(commit.id).toBe(commitId);
        done();
      };
      const eventStore = new EventStore(hybridPersistence, dispatchService);

      eventStore.openPartition('master').then((partition: Partition) => {
        partition.append(new Commit(commitId, 'master', '1', 0, [])).then((commits) => {
          expect(commits.length).toBe(1);
        });
      });
    });

    it('Should return after all commits are persisted', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const commit1 = new Commit('1234', 'master', 0, []);
      const commit2 = new Commit('5678', 'master', 1, []);

      eventStore.openPartition('master').then((partition: EventStorePartition) => {
        partition.append([commit1, commit2]).then((commits: CommitType<Record<string, any>>[]) => {
          expect(commits.length).toBe(2);
          expect(commits[0].id).toBe(commit1.id);
          expect(commits[1].id).toBe(commit2.id);
          done();
        });
      });
    });
  });

  describe('#loadSnapshot', () => {
    it('Should fallback to remote partition if local does not contain snapshot', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = jest.fn(mockLocalPartition.loadSnapshot);
      mockLocalPartition.loadSnapshot = loadSnapshotMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockRemotePartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1).then(() => {
          partition.loadSnapshot(snapshotId).then((snapshot: Record<string, any>) => {
            expect(loadSnapshotMock.mock.calls.length).toBe(1);
            expect(snapshot.version).toBe(1);
            done();
          });
        });
      });
    });

    it('Should automatically store snapshot from remote to local', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = jest.fn(mockLocalPartition.loadSnapshot);
      mockLocalPartition.loadSnapshot = loadSnapshotMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockRemotePartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1).then(() => {
          partition.loadSnapshot(snapshotId).then((snapshot: Record<string, any>) => {
            mockLocalPartition.loadSnapshot(snapshotId).then((localSnapshot: Record<string, any>) => {
              delete localSnapshot.storedDateTime;
              delete snapshot.storedDateTime;

              expect(localSnapshot).toEqual(snapshot);
              done();
            });
          });
        });
      });
    });

    it('Should use local partition if it contains snapshot and TTL has not expired', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadSnapshotMock = jest.fn(mockRemotePartition.loadSnapshot);
      mockRemotePartition.loadSnapshot = loadSnapshotMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: '1' }, 1).then(() => {
          partition.loadSnapshot(snapshotId).then((snapshot: Record<string, any> | undefined) => {
            expect(loadSnapshotMock.mock.calls.length).toBe(0);
            expect(snapshot?.version).toBe(1);
            done();
          });
        });
      });
    });

    it('Should use remote partition if local snapshot has passed its TTL', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const loadRemoteSnapshotMock = jest.fn(mockRemotePartition.loadSnapshot);
      mockRemotePartition.loadSnapshot = loadRemoteSnapshotMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'id' }, 1).then(() => {
          // Manipulate time stored to simulate an old snapshot
          const yesterday = new Date();
          yesterday.setDate(yesterday.getDate() - 1);
          changedSnapshotStoredTime(indexedDB, LOCAL_DB_MOCK, snapshotId, yesterday.toISOString()).then(() => {
            partition.loadSnapshot(snapshotId).then((snapshot: Record<string, any> | undefined) => {
              expect(loadRemoteSnapshotMock.mock.calls.length).toBe(1);
              done();
            });
          });
        });
      });
    });
  });

  describe('#queryStream', () => {
    it('Should fallback to remote partition if local does not have stream locally', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = jest.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockRemotePartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] }).then(() => {
          partition.queryStream(snapshotId).then((queryStream: Record<string, any>) => {
            expect(queryStreamMock.mock.calls.length).toBe(1);
            expect(queryStream.length).toBe(1);
            done();
          });
        });
      });
    });

    it('Should use local partition if it has a snapshot that has not expired', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = jest.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'id' }, 1).then(() => {
          mockLocalPartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] }).then(() => {
            partition.queryStream(snapshotId).then((queryStream) => {
              expect(queryStream.length).toBe(1);
              expect(queryStreamMock.mock.calls.length).toBe(0);
              done();
            });
          });
        });
      });
    });

    it('Should use remote partition if snapshot has expired', (done) => {
      const eventStore = new EventStore(hybridPersistence);
      const snapshotId = 'abc';

      const queryStreamMock = jest.fn(mockRemotePartition.queryStream);
      mockRemotePartition.queryStream = queryStreamMock;

      eventStore.openPartition('master').then((partition: Partition) => {
        mockLocalPartition.storeSnapshot(snapshotId, { version: 1, id: 'a' }, 1).then(() => {
          const yesterday = new Date();
          yesterday.setDate(yesterday.getDate() - 1);
          changedSnapshotStoredTime(indexedDB, LOCAL_DB_MOCK, snapshotId, yesterday.toISOString()).then(() => {
            mockRemotePartition.append({ id: '1', streamId: snapshotId, partitionId: 'master', commitSequence: 1, events: [] }).then(() => {
              partition.queryStream(snapshotId).then((queryStream) => {
                expect(queryStreamMock.mock.calls.length).toBe(1);
                expect(queryStream.length).toBe(1);
                done();
              });
            });
          });
        });
      });
    });
  });
});
