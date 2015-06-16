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
		it('should do nothing if nothing has been appended', function(done) {
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
			var mockPartition = {
				called:false, 
				append:function(commit, callback) {
					this.called=true;
					return Promise.resolve().nodeify(callback);
				},
				_queryStream:function(streamId, callback) {
					return Promise.resolve([]).nodeify(callback);
				}
			};
			var stream = new EventStream(mockPartition, '11');
			stream.append({event:'123'});
			stream.commit(uuid());
			mockPartition.called.should.be.true;
			done();
		});
		it('should keep track of uncommitted events', function(done) {
			var es = new EventStore();
			es.openPartition('location').call('openStream', '1').then(function(stream)
			{
				stream.append({event:'123'});
				stream.getUncommittedEvents().length.should.equal(1);
				done();
			}).catch(function(err) {
				done(err);
			});
		});		
		it('should move uncommitted events to committed on commit', function(done) {
			var es = new EventStore();
			var stream;
			es.openPartition('location').call('openStream', '1').then(function(stream1)
			{
				stream = stream1;
				stream.append({event:'123'});
				return stream.commit(uuid());
			})
			.then(function() {
					stream.getUncommittedEvents().length.should.equal(0);
					stream.getCommittedEvents().length.should.equal(1);
					done();
			}).catch(function(err) {
				done(err);
			});
		});		
		it('two events becomes one commit', function(done) {
			var es = new EventStore();
			var stream;
			es.openPartition('location').call('openStream', '1').then(function(stream1)
			{
				stream = stream1;
				stream.append({event:'123'});
				stream.append({event:'999'});
				return stream.commit(uuid());
			})
			.then(function() {
					stream.getCommittedEvents().length.should.equal(2);
					stream._commitSequence.should.equal(0);
					done();
			}).catch(function(err) {
				done(err);
			});
		});	
		it('two commits gets increasing commit sequence', function(done) {
			var es = new EventStore();
			var stream;
			es.openPartition('location').call('openStream', '1').then(function(stream1)
			{
				stream = stream1;
				stream.append({event:'123'});
				stream.append({event:'999'});
				return stream.commit(uuid());
			})
			.then(function() {
				stream.append({event:'666'});
				stream.append({event:'777'});
				return stream.commit(uuid());
			})
			.then(function() {
					stream.getCommittedEvents().length.should.equal(4);
					stream._commitSequence.should.equal(1);
					done();
			}).catch(function(err) {
				done(err);
			});
		});	
	});
});