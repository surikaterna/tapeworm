var util = require('../lib/client/util');

describe('Util', function() {
	it("#getLastAuthorizedCommitSequence should return -1 for no authorative commit", function() {
		
		var seq = util.getLastAuthorizedCommitSequence([{id:1, commitSequence:0}])
		seq.should.equal(-1);
	});
	it("#getLastAuthorizedCommitSequence should return -1 for no authorative commits", function() {
		var seq = util.getLastAuthorizedCommitSequence([{id:1, commitSequence:0},{id:1, commitSequence:1}])
		seq.should.equal(-1);
	});
	it("#getLastAuthorizedCommitSequence should return 0 for if first commit is authorative", function() {
		var seq = util.getLastAuthorizedCommitSequence([{id:1, commitSequence:0, authorative:true},{id:1, commitSequence:1}])
		seq.should.equal(0);
	});	
	it("#getLastAuthorizedCommitSequence should return 1 for if first commit is authorative", function() {
		var seq = util.getLastAuthorizedCommitSequence([{id:1, commitSequence:0, authorative:true},{id:1, commitSequence:1, authorative:true}])
		seq.should.equal(1);
	});		
	it("#getLastCommitSequence should work with one element", function() {
		var seq = util.getLastCommitSequence([{id:1, commitSequence:0}])
		seq.should.equal(0);
	});
	it("#getLastCommitSequence should work with two elements", function() {
		var seq = util.getLastCommitSequence([{id:1, commitSequence:0},{id:1, commitSequence:1}])
		seq.should.equal(1);
	});
	it("#getLastCommitSequence should rwork with two elements with auth flag", function() {
		var seq = util.getLastCommitSequence([{id:1, commitSequence:0, authorative:true},{id:1, commitSequence:1}])
		seq.should.equal(1);
	});	
	it("#getLastCommitSequence should rwork with two elements with multiple auth flag", function() {
		var seq = util.getLastCommitSequence([{id:1, commitSequence:0, authorative:true},{id:1, commitSequence:1, authorative:true}])
		seq.should.equal(1);
	});		
	it("#getLastCommitSequence should return -1 if null commits", function() {
		var seq = util.getLastCommitSequence(null);
		seq.should.equal(-1);
	});		
	it("#getLastCommitSequence should return -1 if 0 commits", function() {
		var seq = util.getLastCommitSequence([]);
		seq.should.equal(-1);
	});		

});
