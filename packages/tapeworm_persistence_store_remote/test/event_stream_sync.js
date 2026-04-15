var Tapeworm = require('tapeworm');
var Commit = require('tapeworm').Commit;

var should = require('should');

var EventStreamSynchronizer = require('..').Client.EventStreamSynchronizer;

function getSynchronizer() {

 	return new EventStreamSynchronizer();
}

describe('EventStreamSynchronizer', function() {
	var tapeworm = null;
	var part = null;
	var synchronizer = null;

	beforeEach(function(done) {
		tapeworm = new Tapeworm();
		tapeworm.openPartition('blondie').then(function(blondie) {
			part = blondie;
			synchronizer = new EventStreamSynchronizer(part);
			done();
		});
	})

	describe('#_processCommits', function() {
		it('should do nothing if commits matches', function(done) {
			var commits = [new Commit('1', 'location', '1', 0, [{id:'1', type:'alloha'}]), new Commit('2', 'location', '1', 1, [{id:'2', type:'alloha'}])];
			part.append(commits).then(function() {
				synchronizer._processCommits(commits, '1').then(function(conflict) {
					part.openStream('1').then(function(stream) {
						stream.getVersion().should.equal(2);
						done();
					});
				});
			});
		});
		it('should do append if seq is ok', function(done) {
			var commits = [new Commit('1', 'location', '1', 0, [{id:'1', type:'alloha'}]), new Commit('2', 'location', '1', 1, [{id:'2', type:'alloha'}])];
			part.append(commits).then(function() {
				var commit = new Commit('3', 'location', '1', 2, [{id:'3', type:'alloha'}]);
				synchronizer._processCommits([commit], '1').then(function(conflict) {
					conflict.should.equal(false);
					part.openStream('1').then(function(stream) {
						stream.getVersion().should.equal(3);
						done();
					});
				});
			});
		});		
		it('should do trunk and indicate conflict if new commit', function(done) {
			var commits = [new Commit('1', 'location', '1', 0, [{id:'1', type:'alloha.registered'}]), new Commit('2', 'location', '1', 1, [{id:'2', type:'alloha.amended'}])];
			part.append(commits).then(function() {
				synchronizer._processCommits([new Commit('3', 'location', '1', 1, [{id:'3', type:'alloha.removed'}])], '1').then(function(conflict) {
          conflict.should.equal(true);
					part.openStream('1').then(function(stream) {
						stream.getVersion().should.equal(2);
						done();
					});
				});
			});
		});		
	});
	xdescribe('#_applyNewCommit', function() {

		it('should add if seq ok', function(done) {
			var commits = [new Commit('1', 'location', '1', 0, [{id:'1', type:'alloha'}]), new Commit('2', 'location', '1', 1, [{id:'2', type:'alloha'}])];
			part.append(commits).then(function() {
				synchronizer._applyNewCommit(new Commit('3', 'location', '1', 2, [{id:'1', type:'alloha'}]), 1).then(function() {
					done();
				});
			});
		});
		it('should throw if skipping seq', function(done) {
			var commits = [new Commit('1', 'location', '1', 0, [{id:'1', type:'alloha'}]), new Commit('2', 'location', '1', 1, [{id:'2', type:'alloha'}])];
			part.append(commits).then(function() {
				should.throws(function() {
					synchronizer._applyNewCommit(new Commit('3', 'location', '1', 3, [{id:'1', type:'alloha'}]), 1).then(function() {
					});
				});
				done();
			});
		});

	});

});
