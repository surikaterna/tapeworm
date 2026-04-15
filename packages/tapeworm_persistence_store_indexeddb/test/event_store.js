var should = require('should');
var Promise = require('bluebird');

var EventStore = require('tapeworm');
var IdbPersistenceStore = require('..');
global.IDBKeyRange = require("fake-indexeddb/lib/FDBKeyRange");

var getDb = require('./util');


describe('event_store', function() {
	describe('#openPartition', function(done) {
		it('should return non null partition when using new id', function(done) {
			var es = new EventStore(new IdbPersistenceStore(getDb()));
			es.openPartition('location').then(function(partition) {
				partition.should.not.be.null;
				done();
			}).catch(function(err) {
				done(err);
			});
		});
		it('should return same instance when called multiple times', function(done) {
			var es = new EventStore(new IdbPersistenceStore(getDb()));
			Promise.join(es.openPartition('location'), es.openPartition('location'), function(p1, p2) {
				p1.should.equal(p2);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
		it('should return different instances for different partitionIds', function(done) {
			var es = new EventStore(new IdbPersistenceStore(getDb()));
			Promise.join(es.openPartition('location'), es.openPartition('location2'), function(p1, p2) {
				p1.should.not.equal(p2);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
	});
});