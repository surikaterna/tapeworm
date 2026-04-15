const { MongoClient } = require('mongodb');
const Promise = require("bluebird");
const EventStore = require('tapeworm');
const Store = require('..');
const Commit = EventStore.Commit;
const Event = EventStore.Event;
const PersistenceConcurrencyError = EventStore.ConcurrencyError;
const PersistenceDuplicateCommitError = EventStore.DuplicateCommitError;

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

describe('indexeddb_persistence', () => {
  var _db = null;
  var _client = null;
  const getDb = () => _db;

  beforeAll(async () => {
    const mongoClient = await MongoClient.connect(global.__MONGO_URI__);
    const db = await mongoClient.db('db_test_suite');
    _client = mongoClient;
    _db = db;
  });

  beforeEach(async () => {
    try {
      await _db.collection('tw_1_commits').drop();
      await _db.collection('tw_1_snapshots').drop();
    } catch (e) {
    }
  });

  afterAll(async () => {
    await _client.close();
  })

  describe('#commit', function () {
    test('should accept a commit and store it', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () {
          return partition.queryAll()
        }).then(function (x) {
          expect(x.length).toEqual(1);
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });

    test('commit in one stream is not visible in other', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);

        var events = [new Event(uuid(), 'type2', { test: 22 })];
        var commit = new Commit(uuid(), 'master', '2', 0, events);

        partition.append(commit).then(function () {
          Promise.join(partition.queryStream('1'), partition.queryStream('2'), function (r1, r2) {
            expect(r1.length).toEqual(1);
            expect(r2.length).toEqual(1);
            done();
          }).catch(function (err) {
            done(err);
          });
        });
      });
    });

    test('two commits in one stream are visible', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit, function () {
          var events = [new Event(uuid(), 'type2', { test: 22 })];
          var commit = new Commit(uuid(), 'master', '1', 1, events);
          partition.append(commit, function () {
            partition.queryAll().then(function (res) {
              expect(res.length).toEqual(2);
              done();
            });
          });
        });
      });
    });
  });

  test('query stream from version without fallback', (done) => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) {
        e.version = e.data.version
      }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) {
          e.version = e.data.version
        }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 4).then(res => {
            expect(res[0].events[0].version).toEqual(4);
            expect(res[0].events[1].version).toEqual(5);
            expect(res[0].commitSequence).toEqual(1);
            done();
          })
        });
      });
    });
  });

  test('query stream from version lead to empty results', (done) => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) {
        e.version = e.data.version
      }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) {
          e.version = e.data.version
        }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 6).then(res => {
            expect(res).toEqual([]);
            done();
          })
        });
      });
    });
  });

  test('query stream from version with fallback', (done) => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      var events = [
        new Event(uuid(), 'event-1', { test: 11, version: 0 }),
        new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        new Event(uuid(), 'event-3', { test: 13, version: 2 })
      ];
      events.forEach(function (e) {
        e.version = e.data.version
      }); // fake version...
      var commit = new Commit(uuid(), 'master', streamId, 0, events);
      partition.append(commit).then(function () {
        var events = [
          new Event(uuid(), 'event-4', { test: 14, version: 3 }),
          new Event(uuid(), 'event-5', { test: 15, version: 4 }),
          new Event(uuid(), 'event-6', { test: 16, version: 5 })
        ];
        events.forEach(function (e) {
          e.version = e.data.version
        }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 1, events);
        partition.append(commit).then(function () {
          partition.queryStream(streamId, 2).then((res) => {
            expect(res.length).toEqual(2);
            expect(res[0].events[0].version).toEqual(2);
            expect(res[1].events[2].version).toEqual(5);
            expect(res[0].commitSequence).toEqual(0);
            expect(res[1].commitSequence).toEqual(1);
            done();
          })
        });
      });
    });
  });

  test('#getLatestCommit', (done) => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var streamId = uuid();
      partition.getLatestCommit(streamId).then((res) => {
        expect(res).toBeUndefined();
        var events = [
          new Event(uuid(), 'event-1', { test: 11, version: 0 }),
          new Event(uuid(), 'event-2', { test: 12, version: 1 }),
        ];
        events.forEach(function (e) {
          e.version = e.data.version
        }); // fake version...
        var commit = new Commit(uuid(), 'master', streamId, 0, events);
        partition.append(commit).then(function () {
          var events = [
            new Event(uuid(), 'event-4', { test: 14, version: 3 }),
            new Event(uuid(), 'event-5', { test: 15, version: 4 }),
            new Event(uuid(), 'event-6', { test: 16, version: 5 })
          ];
          events.forEach(function (e) {
            e.version = e.data.version
          }); // fake version...
          var commit = new Commit(uuid(), 'master', streamId, 1, events);
          partition.append(commit).then(function () {
            partition.getLatestCommit(streamId).then((res) => {
              expect(res.commitSequence).toEqual(1);
              expect(res.events.length).toEqual(3);
              expect(res.events[2].data.test).toEqual(16);
              done();
            })
          });
        });
      });
    });
  });

  test('#truncateStreamFrom', (done) => {
    var store = new Store(getDb());
    store.openPartition('1').then(function (partition) {
      var firstStreamId = uuid();
      var secondStreamId = uuid();
      var promises = [];
      promises.push(partition.append(new Commit(uuid(), 'master', firstStreamId, 0, [new Event(uuid(), 'event-1-1', { test: 11, version: 0 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', secondStreamId, 0, [new Event(uuid(), 'event-2-1', { test: 21, version: 0 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', firstStreamId, 1, [new Event(uuid(), 'event-1-2', { test: 12, version: 1 })])));
      promises.push(partition.append(new Commit(uuid(), 'master', secondStreamId, 1, [new Event(uuid(), 'event-2-2', { test: 22, version: 1 })])));

      Promise.all(promises).then(async () => {
        await partition.truncateStreamFrom(firstStreamId, 1);
        var r1 = await partition.queryStream(firstStreamId);
        var r2 = await partition.queryStream(secondStreamId);
        expect(r1.length).toEqual(1);
        expect(r1[0].commitSequence).toEqual(0); // seqeunce 0 should exist
        expect(r2.length).toEqual(2); // secondStream should not be affected

        await partition.truncateStreamFrom(secondStreamId, -1);
        r1 = await partition.queryStream(firstStreamId);
        r2 = await partition.queryStream(secondStreamId);
        expect(r1.length).toEqual(1); // firstStream should not be affected
        expect(r2.length).toEqual(0); // should be empty

        done();
      });
    });
  });

  describe('#snapshot', function () {
    test('should return previously stored snapshot', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (part) {
        part.storeSnapshot('stream1', { iAmSnapshot: true }, 10).then(function (snapshot) {
          part.loadSnapshot('stream1').then(function (newSnapshot) {
            expect(newSnapshot).toEqual(snapshot);
            done();
          });
        });
      });
    });
    test('should remove snapshot', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (part) {
        part.storeSnapshot('stream1', { iAmSnapshot: true }, 10).then(function (snapshot) {
          expect(snapshot.version).toEqual(10);
          part.removeSnapshot('stream1').then(function (updated) {
            expect(updated.version).toEqual(-1);
            done();
          })
        });
      });
    });
  });
  describe('#concurrency', function () {
    test('same commit sequence twice should throw', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        var commit2 = new Commit(uuid(), 'master', '1', 0, events);
        return Promise.all([partition.append(commit), partition.append(commit2)]).then(() => {
          done(new Error("Should have thrown concurrency error"));
        });
      }).catch(function (err) {
        if (err instanceof PersistenceConcurrencyError) {
          done();
        } else {
          done(err);
        }
      })
    });
  });
  describe('#duplicateEvents', function () {
    test('same commit twice should throw', (done) => {
      var store = new Store(getDb());
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', { test: 11 })];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () {
          return partition.append(commit);
        })
          .then(function () {
            done(new Error("Should have DuplicateCommitError"));
          }).catch(PersistenceDuplicateCommitError, function (err) {
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });
  });
  describe('#partition', function () {
    test('getting the same partition twice should return same instance', (done) => {
      var store = new Store(getDb());
      Promise.all([store.openPartition('1'), store.openPartition('1')]).then(([p1, p2]) => {
        expect(p1).toBe(p2);
        done();
      });
    });
    test('not indicating partition name should give master partition', (done) => {
      var store = new Store(getDb());
      Promise.all([store.openPartition(), store.openPartition('master')]).then(([p1, p2]) => {
        expect(p1).toBe(p2);
        done();
      });
    });
  });
});
