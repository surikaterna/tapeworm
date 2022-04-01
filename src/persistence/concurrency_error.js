var util = require('util');

var ConcurrencyError = function(message) {
	Error.call(this);
	this.message = message;
}

util.inherits(ConcurrencyError, Error);

module.exports = ConcurrencyError;