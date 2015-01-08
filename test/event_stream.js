var should = require('should');
var Promise = require('bluebird');
var uuid = require('node-uuid').v4;

var EventStore = require('..');
var EventStream = require('../lib/event_stream');

describe('event_stream', function() {
	describe('#openStream', function(done) {
		it('should return 0 commits for new stream', function(done) {
			var es = new EventStore();
			es.openPartition('location').call('openStream', '1').then(function(stream)
			{
				stream.getCommittedEvents().length.should.equal(0);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
	});
	describe('#commit', function(done) {
		it('should do nothing if no commits has been added', function(done) {
			var es = new EventStore();
			es.openPartition('location').call('openStream', '1').then(function(stream)
			{
				stream.commit(uuid());
				stream.getCommittedEvents().length.should.equal(0);
				done();
			}).catch(function(err) {
				done(err);
			});
		});
		it('should call commit on partition', function(done) {
			var mockPartition = {called:false, append:function(commit, callback) {
				console.log('test' + this);
				this.called=true;
				return Promise.resolve().nodeify(callback);
			}};
			var stream = new EventStream(mockPartition, '11');
			stream.append({event:'123'});
			stream.commit(uuid());
			mockPartition.called.should.be.true;
			done();
		});
	});
});