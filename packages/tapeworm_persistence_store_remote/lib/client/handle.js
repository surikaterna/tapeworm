var Promise = require('bluebird');
var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;

var Handle = function (stop) {
  EventEmitter.call(this);
  var self = this;
  this.stop = stop;
  this.synced = new Promise(function (resolve, reject) {
    self.once('insync', function (commits) {
      resolve(commits);
    });
  });
};

inherits(Handle, EventEmitter);

module.exports = Handle;
