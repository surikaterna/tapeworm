var should = require('should');
var uuid = require("uuid").v4;
var Promise = require("bluebird");

var EventStore = require('tapeworm');

var Store = require('../..');
var Commit = EventStore.Commit;
var Event = EventStore.Event;
var PersistenceConcurrencyError = EventStore.ConcurrencyError;
var PersistenceDuplicateCommitError = EventStore.DuplicateCommitError;

var getDb = require('../util');

describe('indexeddb_persistence', function () {
	describe('#commit', function () {
		it('should accept a commit and store it', function (done) {
			var store = new Store(getDb());
			store.openPartition('1').then(function (partition) {
				var events = [new Event(uuid(), 'type1', { test: 11 })];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit).then(function () { return partition.queryAll() }).then(function (x) {
					x.length.should.equal(1);
					done();
				}).catch(function (err) {
					done(err);
				});
			});
		});

		it('should open the database version 4 and have all objectStores', function (done) {
			const store = new Store(getDb(), 'tenantDbKey');
			store.openPartition('master').then((partition) => {
				partition._db.version.should.equal(4);
				(partition._db.objectStoreNames.includes('commits')).should.equal(true);
				(partition._db.objectStoreNames.includes('snapshots')).should.equal(true);
				(partition._db.objectStoreNames.includes('truncated')).should.equal(true);
				done();
			});
		});

		it('should open and create missing object stores', function (done) {
			const indexedDb = getDb();
			const request = indexedDb.open('tw_tenantDbKey_master', 3);
			request.onsuccess = (event) => {
				const db = event.target.result;
				db.close();

				const store = new Store(indexedDb, 'tenantDbKey');
				store.openPartition('master').then((partition) => {
					partition._db.version.should.equal(4);
					(partition._db.objectStoreNames.includes('commits')).should.equal(true);
					(partition._db.objectStoreNames.includes('snapshots')).should.equal(true);
					(partition._db.objectStoreNames.includes('truncated')).should.equal(true);
					done();
				});
			};
		});

		it('commit in one stream is not visible in other', function (done) {
			var store = new Store(getDb());
			store.openPartition('1').then(function (partition) {
				var events = [new Event(uuid(), 'type1', { test: 11 })];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit);

				var events = [new Event(uuid(), 'type2', { test: 22 })];
				var commit = new Commit(uuid(), 'master', '2', 0, events);
				partition.append(commit);

				Promise.join(partition.queryStream('1'), partition.queryStream('2'), function (r1, r2) {
					r1.length.should.equal(1);
					r2.length.should.equal(1);
					done();
				}).catch(function (err) {
					done(err);
				});

			});
		});

		it('two commits in one stream are visible', function () {
			var store = new Store(getDb());
			store.openPartition('1').then(function (partition) {
				var events = [new Event(uuid(), 'type1', { test: 11 })];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit);
				var events = [new Event(uuid(), 'type2', { test: 22 })];
				var commit = new Commit(uuid(), 'master', '1', 1, events);
				partition.append(commit);
				partition.queryAll().then(function (res) {
					res.length.should.equal(2);
				});
			});
		});
	});
	describe('#concurrency', function () {
		xit('same commit sequence twice should throw', function (done) {
			var store = new Store(getDb());
			store.openPartition('1').then(function (partition) {
				var events = [new Event(uuid(), 'type1', { test: 11 })];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				var commit2 = new Commit(uuid(), 'master', '1', 0, events);
				parition.append(commit).then(function () {
					return parition.append(commit2);
				}).then(function () {
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
		it('getting the same partition twice should return same instance', function (done) {
			var store = new Store(getDb());
			Promise.join(store.openPartition('1'), store.openPartition('1'), function (p1, p2) {
				p1.should.equal(p2);
				done();
			});
		});
		it('not indicating partition name should give master partition', function (done) {
			var store = new Store(getDb());
			Promise.join(store.openPartition(), store.openPartition('master'), function (p1, p2) {
				p1.should.equal(p2);
				done();
			});
		});
	});
});
