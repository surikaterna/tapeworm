var should = require('should');
var uuid = require("node-uuid").v4;
var Promise = require("bluebird");

var Store = require('../../lib/persistence/inmemory/inmemory_persistence');
var Commit = require('../../lib/persistence/commit');
var Event = require('../../lib/event');
var PersistenceConcurrencyError = require('../../lib/persistence/concurrency_error');
var PersistenceDuplicateCommitError = require('../../lib/persistence/duplicate_commit_error');

describe('inmemory_persistence', function() {
	describe('#commit', function() {

		it('should accept a commit and store it', function(done) {
			var store = new Store();
			store.openPartition('1').then(function(partition)
				{
					var events = [new Event(uuid(), 'type1',{test:11})];
					var commit = new Commit(uuid(), 'master', '1', 0, events);
					partition.append(commit).then(function(){return partition.queryAll()}).then(function(x){
						x.length.should.equal(1);
						done();
					}).catch(function(err) {
						done(err);
					});
				});
		});

		it('commit in one stream is not visible in other', function(done) {
			var store = new Store();
			store.openPartition('1').then(function(partition) {
				var events = [new Event(uuid(), 'type1',{test:11})];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit);

				var events = [new Event(uuid(), 'type2',{test:22})];
				var commit = new Commit(uuid(), 'master', '2', 0, events);
				partition.append(commit);

				Promise.join(partition.queryStream('1'), partition.queryStream('2'), function(r1, r2){
					r1.length.should.equal(1);
					r2.length.should.equal(1);
					done();
				}).catch(function(err) {
					done(err);
				});

			});
		});

		it('two commits in one stream are visible', function() {
			var store = new Store();
			store.openPartition('1').then(function(partition) {
				var events = [new Event(uuid(), 'type1',{test:11})];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit);
				var events = [new Event(uuid(), 'type2',{test:22})];
				var commit = new Commit(uuid(), 'master', '1', 1, events);
				partition.append(commit);				
				partition.queryAll().then(function(res) {
					res.length.should.equal(2);		
				});
			});
		});
	});
	describe.only('#concurrency', function() {
		it('should throw because the same commit sequence twice was used and no merge logic is available', function(done) {
			var store = new Store();
			store.openPartition('1').then(function(partition) {
				var events = [new Event(uuid(), 'type1',{test:11})];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				var commit2 = new Commit(uuid(), 'master', '1', 0, events);
				return Promise.join(partition.append(commit), partition.append(commit2), function() {
					done(new Error("Should have thrown concurrency error"));
				});
			}).catch(PersistenceConcurrencyError, function(err) {
				done();
			}).catch(function(err) {
				console.log('err' + err);
				done(err);
			});
		});
		it('should not throw even though the same commit sequence twice was used since merge logic is available and returns true', function(done) {
			var store = new Store();
			store.openPartition('1').then(function(partition) {
				var eventStream1 = [new Event(uuid(), 'exampleAggregate.exampleEvent.event',{ orderLineId: 11 })];
				var eventStream2 = [new Event(uuid(), 'exampleAggregate.exampleEvent.event',{ orderLineId: 22 })];
				var commit = new Commit(uuid(), 'master', '1', 0, eventStream1);
				var commit2 = new Commit(uuid(), 'master', '1', 0, eventStream2);
				return Promise.join(partition.append(commit), partition.append(commit2), function() {
					done();
				});
			}).catch(PersistenceConcurrencyError, function(err) {
				throw new Error("Should not have thrown concurrency error")
			}).catch(function(err) {
				console.log('err' + err);
				done(err);
			});
		});
	});
	describe('#duplicateEvents', function() {
		it('same commit twice should throw', function(done) {
			var store = new Store();
			store.openPartition('1').then(function(partition) {
				var events = [new Event(uuid(), 'type1',{test:11})];
				var commit = new Commit(uuid(), 'master', '1', 0, events);
				partition.append(commit).then(function() {
					return partition.append(commit);
				})
				.then(function() {
					done(new Error("Should have DuplicateCommitError"));
				}).catch(PersistenceDuplicateCommitError, function(err) {
					done();
				}).catch(function(err) {
					done(err);
				});
			});
		});
	});
	describe('#partition', function() {
		it('getting the same partition twice should return same instance', function(done) {
			var store = new Store();
			Promise.join(store.openPartition('1'), store.openPartition('1'), function(p1,p2) {
				p1.should.equal(p2);
				done();
			});
		});
		it('not indicating partition name should give master partition', function(done) {
			var store = new Store();
			Promise.join(store.openPartition(), store.openPartition('master'), function(p1,p2) {
				p1.should.equal(p2);
				done();
			});
		});		
	});
});
