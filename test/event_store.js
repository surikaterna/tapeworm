import Promise from 'bluebird';
import {EventStore} from '../src/EventStore';

describe('event_store', function() {
	describe('#openPartition', function(done) {
		it('should return non null partition when using new id', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				expect(partition).not.toBeNull();
				done();
			}).catch(function(err) {
				done(err);
			});
		});
		it('should return same instance when called multiple times', function(done) {
			var es = new EventStore();
			Promise.join(es.openPartition('location'), es.openPartition('location'), function(p1, p2) {
				expect(p1).toEqual(p2);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
		it('should return different instances for different partitionIds', function(done) {
			var es = new EventStore();
			Promise.join(es.openPartition('location'), es.openPartition('location2'), function(p1, p2) {
				expect(p1).not.toEqual(p2);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
	});
});