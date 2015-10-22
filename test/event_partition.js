var should = require('should');
var Promise = require('bluebird');

var EventStore = require('..');
var Commit = EventStore.Commit;

describe('Partition', function() {
	describe('#append', function(done) {
		it('should return commit if added', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				partition.append(new Commit('1', 'location', '1', 0, [])).then(function(c) {
					c[0].events.length.should.equal(0);
					done();
				});
			}).catch(function(err) {
				done(err);
			});
		});
		it('should return after all commits are persisted', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				return partition.append([new Commit('1', 'location', '1', 0, []), new Commit('2', 'location', '1', 1, [])]).then(function(c) {
					c.length.should.equal(2);
					done();
				});
			}).catch(function(err) {
				done(err);
			});
		});
	});
	describe('#_truncateStreamFrom', function() {
		it('should remove all commits ', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				return partition.append([new Commit('1', 'location', '1', 0, []), new Commit('2', 'location', '1', 1, [])]).then(function(c) {
					return partition._truncateStreamFrom('1', 0).then(function() {
						return partition.openStream('1').then(function(stream) {
							stream.getVersion().should.equal(-1);
							done();
						});
					});
				});
			}).catch(function(err) {
				done(err);
			});
		});
		it('should remove all commits after commitSequence', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				return partition.append([new Commit('1', 'location', '1', 0, [{}]), new Commit('2', 'location', '1', 1, [{}])]).then(function(c) {
					return partition._truncateStreamFrom('1', 1).then(function() {
						return partition.openStream('1').then(function(stream) {
							stream.getVersion().should.equal(1);
							done();
						});
					});
				});
			}).catch(function(err) {
				done(err);
			});
		});	
	});
	describe('#_applyCommitHeader', function() {
		it('should add to commit ', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				var commit = new Commit('1', 'location', '1', 0, []);
				return partition.append([commit, new Commit('2', 'location', '1', 1, [])]).then(function(c) {
					return partition._applyCommitHeader(commit.id, {authorative:true}).then(function(commit) {
						commit.authorative.should.be.ok;
						done();
					});
				});
			}).catch(function(err) {
				done(err);
			});
		});
		it('should throw if commit id is unknown', function(done) {
			var es = new EventStore();
			es.openPartition('location').then(function(partition) {
				var commit = new Commit('1', 'location', '1', 0, []);
				return partition.append([commit, new Commit('2', 'location', '1', 1, [])]).then(function(c) {
					return partition._applyCommitHeader("ID MISSING", {authorative:true}).then(function(commit) {
						done(new Error("Unreachable code"));
					
					});
				});
			}).catch(function(err) {
				done();
			});
		});	
	});
});
