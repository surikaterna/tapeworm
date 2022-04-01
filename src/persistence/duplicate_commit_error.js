var util = require('util');

var DuplicateCommitError = function(message) {
	Error.call(this);
	this.message = message;
}

util.inherits(DuplicateCommitError, Error);

module.exports = DuplicateCommitError;