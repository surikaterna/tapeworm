var should = require('should');

var EventStreamSubscriber = require('..').Client.EventStreamSubscriber;

function getSubscriber() {
  var client = {subscribe: function() {return {stop:function() {}}}, request: function (opts, cb) { cb(null, {commits:[]})}};
  var versionProvider = function () {
    return Promise.resolve(0);
  };
  return new EventStreamSubscriber(client, versionProvider, function () {});
}

describe('EventStreamsubhronizer', function() {
  it("#subscribe should return a handle", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    handle.should.be.ok;
  });
  it("#subscribe should return a handle which you can use to stop subscription", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    handle.stop.should.be.ok
  });
  it("#subscribe stop should remove subscriptions", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    sub.activeStreams().length.should.equal(1);
    handle.stop();
    sub.activeStreams().length.should.equal(0);
  });
  it("#subscribe stop multiple times should throw", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    handle.stop();
    should.throws(function() { handle.stop(); });
  });
  it("#subscribe multiple times should not increase activeStreams", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    var handle2 = sub.subscribe('stream-1');
    sub.activeStreams().length.should.equal(1);
  });
  it("#stop with multiple subscriptions should not decrease activeStreams", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    var handl2 = sub.subscribe('stream-1');
    handle.stop();
    sub.activeStreams().length.should.equal(1);
  });
  it("#subscribe on multiple streams increases activeStreams", function() {
    var sub = getSubscriber();
    var handle = sub.subscribe('stream-1');
    var handl2 = sub.subscribe('stream-2');
    sub.activeStreams().length.should.equal(2);
  });
});
