import {v4 as uuid} from "uuid";
import Promise from "bluebird";
import Store from '../../src/persistence/inmemory/InMemoryPersistence';
import {Commit} from '../../src/persistence/Commit';
import Event from '../../src/Event';
import {ConcurrencyError as PersistenceConcurrencyError} from '../../src/persistence/ConcurrencyError';
import {DuplicateCommitError as PersistenceDuplicateCommitError} from '../../src/persistence/DuplicateCommitError';

describe('inmemory_persistence', function () {
  describe('#commit', function () {

    it('should accept a commit and store it', function (done) {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit).then(function () {
          return partition.queryAll()
        }).then(function (x) {
          expect(x).toHaveLength(1);
          done();
        }).catch(function (err) {
          done(err);
        });
      });
    });

    it('commit in one stream is not visible in other', function (done) {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);

        var events = [new Event(uuid(), 'type2', {test: 22})];
        var commit = new Commit(uuid(), 'master', '2', 0, events);
        partition.append(commit);

        Promise.join(partition.queryStream('1'), partition.queryStream('2'), function (r1, r2) {
          expect(r1).toHaveLength(1);
          expect(r2).toHaveLength(1);
          done();
        }).catch(function (err) {
          done(err);
        });

      });
    });

    it('two commits in one stream are visible', function () {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);
        var events = [new Event(uuid(), 'type2', {test: 22})];
        var commit = new Commit(uuid(), 'master', '1', 1, events);
        partition.append(commit);
        partition.queryAll().then(function (res) {
          expect(res).toHaveLength(2);
        });
      });
    });

    it('should skip events', function () {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11}), new Event(uuid(), 'type1', {test: 12})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);
        var events = [new Event(uuid(), 'type2', {test: 22})];
        var commit = new Commit(uuid(), 'master', '1', 1, events);
        partition.append(commit);
        partition.queryStream('1', 2).then(function (res) {
          expect(res).toHaveLength(1);
        });
      });
    });
    it('should skip events and split commit if inbetween', function () {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11}), new Event(uuid(), 'type1', {test: 12})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        partition.append(commit);
        var events = [new Event(uuid(), 'type2', {test: 22})];
        var commit = new Commit(uuid(), 'master', '1', 1, events);
        partition.append(commit);
        partition.queryStream('1', 1).then(function (res) {
          expect(res).toHaveLength(2);
          expect(res[0].events).toHaveLength(1);
        });
      });
    });
  });
  describe('#concurrency', function () {
    it('same commit sequence twice should throw', function (done) {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11})];
        var commit = new Commit(uuid(), 'master', '1', 0, events);
        var commit2 = new Commit(uuid(), 'master', '1', 0, events);
        return Promise.join(partition.append(commit), partition.append(commit2), function () {
          done(new Error("Should have thrown concurrency error"));
        });
      }).catch(PersistenceConcurrencyError, function (err) {
        done();
      }).catch(function (err) {
        console.log('err' + err);
        done(err);
      });
    });
  });
  describe('#duplicateEvents', function () {
    it('same commit twice should throw', function (done) {
      var store = new Store();
      store.openPartition('1').then(function (partition) {
        var events = [new Event(uuid(), 'type1', {test: 11})];
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
    it('getting the same partition twice should return same instance', function (done) {
      var store = new Store();
      Promise.join(store.openPartition('1'), store.openPartition('1'), function (p1, p2) {
        expect(p1).toEqual(p2);
        done();
      });
    });
    it('not indicating partition name should give master partition', function (done) {
      var store = new Store();
      Promise.join(store.openPartition(), store.openPartition('master'), function (p1, p2) {
        expect(p1).toEqual(p2);
        done();
      });
    });
  });
  describe('#storeSnapshot', function () {
    it('should return previously stored snapshot', function (done) {
      var store = new Store();
      store.openPartition('1').then(function (part) {
        part.storeSnapshot('stream1', {iAmSnapshot: true}, 10).then(function (snapshot) {
          part.loadSnapshot('stream1').then(function (newSnapshot) {
            expect(newSnapshot).toEqual(snapshot);
            done();
          });
        });
      });
    });

  });
});
